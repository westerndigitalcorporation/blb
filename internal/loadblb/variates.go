// Copyright (c) 2016 Western Digital Corporation or its affiliates. All rights reserved.
// SPDX-License-Identifier: MIT

package loadblb

import (
	"encoding/json"
	"math"
	"math/rand"
	"sync"

	log "github.com/golang/glog"
)

// VariateConfig includes a Variate name, a seed, and additional parameters
// encoded in a raw JSON object.
type VariateConfig struct {
	Name       string          // Name of the variate.
	Seed       int64           // Seed for pseudo-random number generators, if any.
	Parameters json.RawMessage // Parameters in raw encoded JSON object.
}

// Parse parses the encoded variate.
func (vc *VariateConfig) Parse() Variate {
	var v Variate
	switch vc.Name {
	case "Constant":
		var p float64
		json.Unmarshal(vc.Parameters, &p)
		v = NewConstant(p)

	case "Uniform":
		var p UniformParameters
		json.Unmarshal(vc.Parameters, &p)
		v = NewUniform(vc.Seed, p.Lower, p.Upper)

	case "Exponential":
		var p float64
		json.Unmarshal(vc.Parameters, &p)
		v = NewExponential(vc.Seed, p)

	case "Normal":
		var p NormalParameters
		json.Unmarshal(vc.Parameters, &p)
		v = NewNormal(vc.Seed, p.Mean, p.Stddev)

	case "Poisson":
		var p float64
		json.Unmarshal(vc.Parameters, &p)
		v = NewPoisson(vc.Seed, p)

	case "Pareto":
		var p ParetoParameters
		json.Unmarshal(vc.Parameters, &p)
		v = NewPareto(vc.Seed, p.Xm, p.Alpha)

	default:
		log.Fatalf("unknown variate name: %s", vc.Name)
	}
	return v
}

// Variate represents a random variable following certain
// distribution and is used to model system traffic.
type Variate interface {
	// Sample gets a sample of the random distribution.
	Sample() float64
}

//====== Constant distribution ======//

// Constant always returns a constant value.
type Constant struct {
	v float64
}

// NewConstant returns a new constant distribution.
func NewConstant(v float64) *Constant {
	return &Constant{v: v}
}

// Sample implements Variate.
func (c *Constant) Sample() float64 {
	return c.v
}

//====== Uniform distribution ======//

// UniformParameters defines parameters for Uniform.
type UniformParameters struct {
	Lower, Upper int64
}

// Uniform generates uniform samples in [lb, ub).
type Uniform struct {
	r  *rand.Rand
	l  sync.Mutex
	lb int64
	ub int64
}

// NewUniform returns a new uniform variate. 'lb' must be smaller than 'ub'.
func NewUniform(seed, lb, ub int64) *Uniform {
	if lb >= ub {
		panic("lower bound should be smaller than upper bound")
	}
	return &Uniform{
		r:  rand.New(rand.NewSource(seed)),
		lb: lb,
		ub: ub,
	}
}

// Sample implements Variate.
func (u *Uniform) Sample() float64 {
	u.l.Lock()
	defer u.l.Unlock()
	return float64(u.r.Int63n(u.ub-u.lb) + u.lb)
}

//====== Exponential distribution ======//

// Exponential implements a sampling algorithm for Exponential distribution
// using inverse transform sampling.
type Exponential struct {
	r      *rand.Rand
	l      sync.Mutex
	lambda float64 // Rate paramenter.
}

// NewExponential returns a new Exponential variate. 'lambda' must be positive.
func NewExponential(seed int64, lambda float64) *Exponential {
	if lambda <= 0 {
		panic("rate parameter cannot be non-positive")
	}
	return &Exponential{
		r:      rand.New(rand.NewSource(seed)),
		lambda: lambda,
	}
}

// Sample implements Variate.
func (e *Exponential) Sample() float64 {
	e.l.Lock()
	defer e.l.Unlock()
	return -math.Log(float64Exclusive(e.r)) / e.lambda
}

// float64Exclusive returns, as a float64, a pseudo-random number in (0.0, 1.0)
// (both are exclusive). If 'r' is nil, the default pseudo-random number
// generator in math/rand is used; otherwise, the caller is responsible for
// locking on 'r'.
func float64Exclusive(r *rand.Rand) (s float64) {
	for s == 0.0 {
		if r == nil {
			s = rand.Float64()
			continue
		}
		s = r.Float64()
	}
	return
}

//====== Normal distribution ======//

// NormalParameters defines parameters for Normal.
type NormalParameters struct {
	Mean, Stddev float64
}

// Normal generates samples from the normal distribution N(mean, stddev^2).
type Normal struct {
	r      *rand.Rand
	l      sync.Mutex
	mean   float64 // Mean.
	stddev float64 // Standard deviation.
}

// NewNormal returns a new Normal variate. 'stddev' must be positive.
func NewNormal(seed int64, mean, stddev float64) *Normal {
	if stddev <= 0 {
		panic("standard deviation must be positive")
	}
	return &Normal{
		r:      rand.New(rand.NewSource(seed)),
		mean:   mean,
		stddev: stddev,
	}
}

// Sample implements Variate.
func (n *Normal) Sample() float64 {
	n.l.Lock()
	defer n.l.Unlock()
	return n.r.NormFloat64()*n.stddev + n.mean
}

//====== Poisson distribution ======//

// Poisson implements a simple algorithm by Knuth to generate random
// Poisson-distributed numbers by Knuth. Poisson distribution is suitable for
// modeling traffic with independent events with a known average arrival rate.
//
// This algorithm has a complexity linear in the returned value, which is lambda
// on average. Therefore, it may not be efficient for large values of lambda and
// may have numerical stability issues with the term exp(-lambda). But it should
// be good enough for a simple simulator use.
type Poisson struct {
	r      *rand.Rand
	l      sync.Mutex
	lambda float64 // Average arrival rate.
}

// NewPoisson returns a new Poisson variate. 'lambda' must not be negative.
func NewPoisson(seed int64, lambda float64) *Poisson {
	if lambda < 0 {
		panic("avarage arrival rate cannot be negative")
	}
	return &Poisson{
		r:      rand.New(rand.NewSource(seed)),
		lambda: lambda,
	}
}

// Sample implements Variate.
func (ps *Poisson) Sample() float64 {
	ps.l.Lock()
	defer ps.l.Unlock()

	k := 0
	L := math.Exp(-ps.lambda)
	p := float64Inclusive(ps.r)
	for p > L {
		p *= float64Inclusive(ps.r)
		k++

	}
	return float64(k)
}

// float64Inclusive returns, as a float64, a pseudo-random number in [0.0, 1.0]
// (both are inclusive). If 'r' is nil, the default pseudo-random number
// generator in math/rand is used; otherwise, the caller is responsible for
// locking on 'r'.
func float64Inclusive(r *rand.Rand) float64 {
	if r == nil {
		return float64(rand.Int63()) / (1 << 63)
	}
	return float64(r.Int63()) / (1 << 63)
}

//====== Pareto distribution ======//

// ParetoParameters defines parameters for Pareto.
type ParetoParameters struct {
	Xm, Alpha float64
}

// Pareto implements a sampling algorithm for Pareto distribution using inverse
// transform sampling. Pareto distribution is suitable for modeling bursty
// traffic with long-tails.
type Pareto struct {
	r     *rand.Rand
	l     sync.Mutex
	xm    float64 // Scale parameter.
	alpha float64 // Tail index.
}

// NewPareto returns a new Pareto variate. 'xm' and 'alpha' must be positive.
func NewPareto(seed int64, xm, alpha float64) *Pareto {
	if xm <= 0 {
		panic("scale parameter cannot be non-positive")
	}
	if alpha <= 0 {
		panic("tail index cannot be non-positive")
	}
	return &Pareto{
		r:     rand.New(rand.NewSource(seed)),
		xm:    xm,
		alpha: alpha,
	}
}

// Sample implements Variate.
func (p *Pareto) Sample() float64 {
	p.l.Lock()
	defer p.l.Unlock()
	return p.xm / math.Pow(1.0-p.r.Float64(), 1.0/p.alpha)
}
