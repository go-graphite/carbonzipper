// Package ganglia has convenience functions for using ganglia.
//
// This package uses the vlog utility package for logging various messages
// if the vlog.Verbose boolean is set to true.
package ganglia

import (
	"flag"
	"fmt"
	"html"
	"io/ioutil"
	"log"
	"net"
	"os"
	"regexp"
	"runtime"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/fastly/go-utils/debug"
	"github.com/fastly/go-utils/stopper"
	"github.com/fastly/go-utils/vlog"
	"github.com/jbuchbinder/go-gmetric/gmetric"
)

const (
	String = gmetric.VALUE_STRING
	Ushort = gmetric.VALUE_UNSIGNED_SHORT
	Short  = gmetric.VALUE_SHORT
	Uint   = gmetric.VALUE_UNSIGNED_INT
	Int    = gmetric.VALUE_INT
	Float  = gmetric.VALUE_FLOAT
	Double = gmetric.VALUE_DOUBLE
)

var (
	GmondConfig string
	Interval    time.Duration

	gmondChannelRe  = regexp.MustCompile("udp_send_channel\\s*{([^}]+)}")
	gmondHostPortRe = regexp.MustCompile("(host|port)\\s*=\\s*(\\S+)")

	globalReporter struct {
		sync.Once
		*Reporter
	}
)

func init() {
	flag.StringVar(&GmondConfig, "gmond-config", "/etc/ganglia/gmond.conf", "location of gmond.conf")
	flag.DurationVar(&Interval, "ganglia-interval", 9*time.Second, "time between gmetric updates")
}

type gmetricSample struct {
	value interface{}
	when  time.Time
}
type Reporter struct {
	*stopper.ChanStopper
	prefix    string
	callbacks []ReporterCallback
	mu        sync.Mutex // guards previous
	previous  map[string]gmetricSample
	groupName string
	dmax      uint32
}

// MetricSender takes the following parameters:
//   name: an arbitrary metric name
//   value: the metric's current value
//   metricType: one of GmetricString, GmetricUshort, GmetricShort, GmetricUint, GmetricInt, GmetricFloat, or GmetricDouble
//   units: a label to include on the metric's Y axis
//   rate: if true, send the rate relative to the last sample instead of an absolute value
type MetricSender func(name string, value string, metricType uint32, units string, rate bool)

type ReporterCallback func(MetricSender)

// Gmetric returns a global Reporter that clients may hook into by
// calling AddCallback.
func Gmetric() *Reporter {
	globalReporter.Do(func() {
		globalReporter.Reporter = NewGangliaReporter(Interval)
		globalReporter.AddCallback(CommonGmetrics)
	})
	return globalReporter.Reporter
}

// Configure sets group name and prefix of a reporter and returns the reporter.
func (gr *Reporter) Configure(groupName, prefix string) *Reporter {
	if gr == nil {
		return nil
	}
	gr.prefix = html.EscapeString(prefix)
	gr.groupName = html.EscapeString(groupName)
	return gr
}

// SetDmax configures the amount of time that metrics are valid for in the
// tsdb. The default of 0 means forever. Time resolution is only respected to
// the second.
func (gr *Reporter) SetDmax(dmax time.Duration) *Reporter {
	if gr == nil {
		return nil
	}
	gr.dmax = uint32(dmax.Seconds())
	return gr
}

// Convenience wrapper for Gmetric().AddCallback():
//
//   AddGmetrics(func(gmetric MetricSender) {
// 	   gmetric("profit", "1000000.00", GmetricFloat, "dollars", true)
//   })
func AddGmetrics(callback ReporterCallback) {
	Gmetric().AddCallback(callback)
}

func NewGmetric() (*gmetric.Gmetric, error) {
	b, err := ioutil.ReadFile(GmondConfig)
	if err != nil {
		return nil, err
	}
	stanzas := gmondChannelRe.FindAllStringSubmatch(string(b), -1)
	if len(stanzas) == 0 {
		return nil, fmt.Errorf("No udp_send_channel stanzas found in %s", GmondConfig)
	}

	servers := make([]gmetric.Server, 0)
	for _, stanza := range stanzas {
		var host, port string
		for _, match := range gmondHostPortRe.FindAllStringSubmatch(stanza[1], 2) {
			if match[1] == "host" {
				host = match[2]
			} else if match[1] == "port" {
				port = match[2]
			}
		}
		if host == "" || port == "" {
			return nil, fmt.Errorf("Missing host or port from %s stanza %q", GmondConfig, stanza[0])
		}
		portNum, err := strconv.Atoi(port)
		if err != nil {
			return nil, err
		}
		ips, err := net.LookupIP(host)
		if err != nil {
			return nil, err
		}
		for _, ip := range ips {
			vlog.VLogf("Reporting to Ganglia server at %s:%d", ip, portNum)
			servers = append(servers, gmetric.Server{ip, portNum})
		}
	}

	// see http://sourceforge.net/apps/trac/ganglia/wiki/gmetric_spoofing
	hostname, _ := os.Hostname()
	spoofName := fmt.Sprintf("%s:%s", hostname, hostname)

	gm := gmetric.Gmetric{Spoof: spoofName}
	for _, server := range servers {
		gm.AddServer(server)
	}
	return &gm, nil
}

// NewGangliaReporter returns a Reporter object which calls callback every
// interval with the given group name. callback is passed a Gmetric whose
// servers are initialized from the hosts gmond.conf. Calling Stop on the
// Reporter will cease its operation.
func NewGangliaReporter(interval time.Duration) *Reporter {
	return NewGangliaReporterWithOptions(interval, "")
}

// NewGangliaReporterWithOptions is NewGangliaReporter with the groupName.
func NewGangliaReporterWithOptions(interval time.Duration, groupName string) *Reporter {
	gm, err := NewGmetric()
	if err != nil {
		vlog.VLogfQuiet("ganglia", "Couldn't start Ganglia reporter: %s", err)
		return nil
	} else if gm == nil {
		return nil
	}
	stopper := stopper.NewChanStopper()
	gr := &Reporter{
		ChanStopper: stopper,
		prefix:      "",
		callbacks:   []ReporterCallback{},
		previous:    make(map[string]gmetricSample),
		groupName:   groupName,
		dmax:        0,
	}
	go func() {
		defer stopper.Done()
		for {
			select {
			case <-stopper.Chan:
				return
			case <-time.After(interval):
				go func() {
					// SendMetric "opens" and "closes" UDP connections each
					// time, but since we expect the callback to send several
					// metrics at once, avoid that here.
					conns := gm.OpenConnections()
					n := 0
					sender := func(name string, value string, metricType uint32, units string, rate bool) {
						v := value
						if rate {
							gr.mu.Lock()
							defer gr.mu.Unlock()
							prev, exists := gr.previous[name]
							units += "/sec"

							now := time.Now()

							switch metricType {
							case Ushort, Short, Uint, Int:
								i, err := strconv.Atoi(value)
								if err != nil {
									vlog.VLogfQuiet(name, "Value %q doesn't look like an int: %s", value, err)
									return
								}
								gr.previous[name] = gmetricSample{i, now}
								if !exists {
									return
								}
								delta := i - prev.value.(int)
								elapsed := time.Now().Sub(prev.when).Seconds()
								v = fmt.Sprint(float64(delta) / elapsed)
								// upgrade to a float to avoid loss of precision
								metricType = Float

							case Float, Double:
								f, err := strconv.ParseFloat(value, 64)
								if err != nil {
									vlog.VLogfQuiet(name, "Value %q doesn't look like a float: %s", value, err)
									return
								}
								gr.previous[name] = gmetricSample{f, now}
								if !exists {
									return
								}
								delta := f - prev.value.(float64)
								elapsed := time.Now().Sub(prev.when).Seconds()
								v = fmt.Sprint(delta / elapsed)

							case String:
								vlog.VLogfQuiet(name, "Can't compute deltas for string metric %q", value)
								return
							}
						}

						// gmetad fails to escape quotes, eventually generating
						// invalid xml. do it here as a workaround.
						v = html.EscapeString(v)
						name = html.EscapeString(name)
						units = html.EscapeString(units)

						n++
						gm.SendMetricPackets(
							gr.prefix+name, v, metricType, units,
							gmetric.SLOPE_BOTH,
							uint32(interval.Seconds()), // tmax is the expected reporting interval
							gr.dmax,
							gr.groupName,
							gmetric.PACKET_BOTH, conns,
						)
						if debug.On() {
							if rate {
								log.Printf("gmetric: name=%q, rate=%q, value=%q, type=%d, units=%q, slope=%d, tmax=%d, dmax=%v, group=%q, packet=%d",
									gr.prefix+name, v, value, metricType, units, gmetric.SLOPE_BOTH,
									uint32(interval.Seconds()), gr.dmax, gr.groupName, gmetric.PACKET_BOTH,
								)
							} else {
								log.Printf("gmetric: name=%q, value=%q, type=%d, units=%q, slope=%d, tmax=%d, dmax=%v, group=%q, packet=%d",
									gr.prefix+name, v, metricType, units, gmetric.SLOPE_BOTH,
									uint32(interval.Seconds()), gr.dmax, gr.groupName, gmetric.PACKET_BOTH,
								)
							}
						}
					}
					defer gm.CloseConnections(conns)
					for _, callback := range gr.callbacks {
						callback(sender)
					}
					if debug.On() {
						log.Printf("Published %d metrics to Ganglia", n)
					}
				}()
			}
		}
	}()
	return gr
}

func (gr *Reporter) AddCallback(callback ReporterCallback) {
	if gr == nil {
		return
	}
	gr.callbacks = append(gr.callbacks, callback)
}

func (g *Reporter) Stop() {
	if g == nil {
		return
	}
	g.ChanStopper.Stop()
}

func CommonGmetrics(gmetric MetricSender) {
	gmetric("goroutines", fmt.Sprintf("%d", runtime.NumGoroutine()), Uint, "num", false)

	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	gmetric("mem_alloc", fmt.Sprintf("%d", mem.Alloc), Uint, "bytes", false)
	gmetric("mem_sys", fmt.Sprintf("%d", mem.Sys), Uint, "bytes", false)
	gmetric("mem_gc_pause_last", fmt.Sprintf("%.6f", float64(mem.PauseNs[(mem.NumGC+255)%256])/1e6), Float, "ms", false)
	var gcPauseMax uint64
	for _, v := range mem.PauseNs {
		if v > gcPauseMax {
			gcPauseMax = v
		}
	}
	gmetric("mem_gc_pause_max", fmt.Sprintf("%.6f", float64(gcPauseMax)/1e6), Float, "ms", false)
	gmetric("mem_gc_pause_total", fmt.Sprintf("%.6f", float64(mem.PauseTotalNs)/1e6), Float, "ms", true)
	since := time.Now().Sub(time.Unix(0, int64(mem.LastGC))).Seconds()
	gmetric("mem_gc_pause_since", fmt.Sprintf("%.6f", since), Float, "sec", false)

	var r syscall.Rusage
	if syscall.Getrusage(syscall.RUSAGE_SELF, &r) == nil {
		gmetric("rusage_utime", fmt.Sprintf("%.6f", float64(r.Utime.Nano())/1e9), Float, "cpusecs", true)
		gmetric("rusage_stime", fmt.Sprintf("%.6f", float64(r.Stime.Nano())/1e9), Float, "cpusecs", true)
		gmetric("cpu_pct", fmt.Sprintf("%.4f", 100*float64((r.Utime.Nano()+r.Stime.Nano()))/1e9), Float, "%", true)
	}
}
