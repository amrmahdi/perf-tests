/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package common

import (
	"fmt"
	"math"
	"sort"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"k8s.io/perf-tests/clusterloader2/pkg/measurement"
	measurementutil "k8s.io/perf-tests/clusterloader2/pkg/measurement/util"
	"k8s.io/perf-tests/clusterloader2/pkg/util"
)

const (
	bindThroughputMeasurementName = "SchedulingThroughputBindTime"
)

func init() {
	if err := measurement.Register(bindThroughputMeasurementName, createBindThroughputMeasurement); err != nil {
		klog.Fatalf("Cannot register %s: %v", bindThroughputMeasurementName, err)
	}
}

func createBindThroughputMeasurement() measurement.Measurement {
	return &bindThroughputMeasurement{}
}

// bindThroughputMeasurement computes scheduling throughput by bucketing each pod
// by the second of its PodScheduled condition's LastTransitionTime (the time the
// scheduler stamped the bind), rather than by client watch-observation time.
//
// Compared to the SchedulingThroughput measurement (which polls a pod store and
// diffs counts on the client's own clock), bucketing by the scheduler-stamped
// transition time removes client watch-delivery lag from the per-second curve, so
// the per-second peaks are placed at the true bind time. It is portable across
// providers because it reads only the pod object (no scheduler endpoint needed).
//
// Limitation: PodScheduled LastTransitionTime is second-resolution, so this is
// inherently a per-second signal and must not be used for sub-second latency.
type bindThroughputMeasurement struct {
	podStore  *measurementutil.PodStore
	selector  *util.ObjectSelector
	isRunning bool
}

type perSecondPoint struct {
	UnixSecond int64 `json:"unixSecond"`
	Scheduled  int   `json:"scheduled"`
}

type bindThroughput struct {
	Perc50         float64          `json:"perc50"`
	Perc90         float64          `json:"perc90"`
	Perc99         float64          `json:"perc99"`
	Max            float64          `json:"max"`
	TotalScheduled int              `json:"totalScheduled"`
	WindowSeconds  int              `json:"windowSeconds"`
	PerSecond      []perSecondPoint `json:"perSecond"`
}

// Execute supports two actions:
//   - start - begins watching pods matching the selector.
//   - gather - lists the matched pods, buckets them by PodScheduled transition
//     second, and emits the per-second throughput series plus percentiles.
func (b *bindThroughputMeasurement) Execute(config *measurement.Config) ([]measurement.Summary, error) {
	action, err := util.GetString(config.Params, "action")
	if err != nil {
		return nil, err
	}
	switch action {
	case "start":
		if b.isRunning {
			klog.V(3).Infof("%s: measurement already running", b)
			return nil, nil
		}
		b.selector = util.NewObjectSelector()
		if err := b.selector.Parse(config.Params); err != nil {
			return nil, err
		}
		ps, err := measurementutil.NewPodStore(config.ClusterFramework.GetClientSets().GetClient(), b.selector)
		if err != nil {
			return nil, fmt.Errorf("pod store creation error: %v", err)
		}
		b.podStore = ps
		b.isRunning = true
		klog.V(2).Infof("%s: started watching %s", b, b.selector.String())
		return nil, nil
	case "gather":
		return b.gather()
	default:
		return nil, fmt.Errorf("unknown action %v", action)
	}
}

func (b *bindThroughputMeasurement) gather() ([]measurement.Summary, error) {
	if !b.isRunning {
		return nil, fmt.Errorf("measurement %s has not been started", bindThroughputMeasurementName)
	}
	pods, err := b.podStore.List()
	if err != nil {
		return nil, fmt.Errorf("unexpected error on PodStore.List: %w", err)
	}

	counts := make(map[int64]int)
	total := 0
	var minSec, maxSec int64
	first := true
	for _, pod := range pods {
		sec, ok := podScheduledUnixSecond(pod)
		if !ok {
			continue
		}
		counts[sec]++
		total++
		if first || sec < minSec {
			minSec = sec
		}
		if first || sec > maxSec {
			maxSec = sec
		}
		first = false
	}

	summary := &bindThroughput{TotalScheduled: total}
	if total > 0 {
		// Build a dense per-second series over the active window, filling idle
		// seconds with 0 so percentiles reflect the whole scheduling window.
		var perSecondCounts []float64
		for s := minSec; s <= maxSec; s++ {
			c := counts[s]
			summary.PerSecond = append(summary.PerSecond, perSecondPoint{UnixSecond: s, Scheduled: c})
			perSecondCounts = append(perSecondCounts, float64(c))
		}
		summary.WindowSeconds = int(maxSec-minSec) + 1
		sorted := make([]float64, len(perSecondCounts))
		copy(sorted, perSecondCounts)
		sort.Float64s(sorted)
		summary.Perc50 = percentile(sorted, 50)
		summary.Perc90 = percentile(sorted, 90)
		summary.Perc99 = percentile(sorted, 99)
		summary.Max = sorted[len(sorted)-1]
	}

	content, err := util.PrettyPrintJSON(summary)
	if err != nil {
		return nil, err
	}
	klog.V(2).Infof("%s: %d pods scheduled over %ds; peak %.0f/s, p50 %.0f/s", b, total, summary.WindowSeconds, summary.Max, summary.Perc50)
	return []measurement.Summary{measurement.CreateSummary(bindThroughputMeasurementName, "json", content)}, nil
}

// podScheduledUnixSecond returns the unix second of the pod's PodScheduled=True
// transition, or false if the pod has not been scheduled.
func podScheduledUnixSecond(pod *corev1.Pod) (int64, bool) {
	for i := range pod.Status.Conditions {
		c := &pod.Status.Conditions[i]
		if c.Type == corev1.PodScheduled && c.Status == corev1.ConditionTrue {
			return c.LastTransitionTime.Unix(), true
		}
	}
	return 0, false
}

func percentile(sorted []float64, p int) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(float64(len(sorted)*p)/100)) - 1
	if idx < 0 {
		idx = 0
	}
	return sorted[idx]
}

// Dispose cleans up after the measurement.
func (b *bindThroughputMeasurement) Dispose() {
	if b.isRunning {
		b.isRunning = false
		if b.podStore != nil {
			b.podStore.Stop()
		}
	}
}

// String returns a string representation of the measurement.
func (*bindThroughputMeasurement) String() string {
	return bindThroughputMeasurementName
}
