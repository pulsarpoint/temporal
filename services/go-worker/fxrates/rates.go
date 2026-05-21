package fxrates

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"math"
	"net/http"
	"time"
)

const ecbURL = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml"
const ecbHistoricalURL = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.xml"

type Rates struct {
	eurPer   map[string]float64
	rateDate time.Time
}

func Load(ctx context.Context) (*Rates, error) {
	return LoadFrom(ctx, ecbURL)
}

func LoadForDate(ctx context.Context, date time.Time) (*Rates, error) {
	return LoadHistoricalFrom(ctx, ecbHistoricalURL, date)
}

func LoadFrom(ctx context.Context, url string) (*Rates, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch ecb: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("ecb returned %d", resp.StatusCode)
	}
	return parse(resp.Body)
}

func LoadHistoricalFrom(ctx context.Context, url string, date time.Time) (*Rates, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch historical ecb: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("historical ecb returned %d", resp.StatusCode)
	}
	return parseForDate(resp.Body, date)
}

type envelope struct {
	Cube outerCube `xml:"Cube"`
}

type outerCube struct {
	Days []dailyCube `xml:"Cube"`
}

type dailyCube struct {
	Time  string     `xml:"time,attr"`
	Rates []rateCube `xml:"Cube"`
}

type rateCube struct {
	Currency string  `xml:"currency,attr"`
	Rate     float64 `xml:"rate,attr"`
}

func parse(r io.Reader) (*Rates, error) {
	rates, err := parseRates(r, nil)
	if err != nil {
		return nil, err
	}
	return rates, nil
}

func parseForDate(r io.Reader, date time.Time) (*Rates, error) {
	target := date.UTC()
	rates, err := parseRates(r, &target)
	if err != nil {
		return nil, err
	}
	return rates, nil
}

func parseRates(r io.Reader, target *time.Time) (*Rates, error) {
	var env envelope
	if err := xml.NewDecoder(r).Decode(&env); err != nil {
		return nil, fmt.Errorf("parse ecb xml: %w", err)
	}

	var selected *dailyCube
	for i := range env.Cube.Days {
		day := &env.Cube.Days[i]
		if day.Time == "" {
			continue
		}
		if target == nil {
			selected = day
			break
		}
		parsed, err := time.Parse("2006-01-02", day.Time)
		if err != nil {
			return nil, fmt.Errorf("parse ecb rate date: %w", err)
		}
		if !parsed.After(*target) && (selected == nil || day.Time > selected.Time) {
			selected = day
		}
	}
	if selected == nil {
		if target != nil {
			return nil, fmt.Errorf("ECB rate not found on or before %s", target.Format("2006-01-02"))
		}
		return nil, fmt.Errorf("ECB feed did not contain rates")
	}

	rateDate := time.Now().UTC()
	if selected.Time != "" {
		parsed, err := time.Parse("2006-01-02", selected.Time)
		if err != nil {
			return nil, fmt.Errorf("parse ecb rate date: %w", err)
		}
		rateDate = parsed
	}
	eurPer := make(map[string]float64, len(selected.Rates)+1)
	eurPer["EUR"] = 1
	for _, rc := range selected.Rates {
		eurPer[rc.Currency] = rc.Rate
	}
	return &Rates{eurPer: eurPer, rateDate: rateDate}, nil
}

func (r *Rates) ToUSDCents(amount float64, currency string) (int64, error) {
	if currency == "USD" {
		return int64(math.Round(amount * 100)), nil
	}
	usdRate, ok := r.eurPer["USD"]
	if !ok {
		return 0, fmt.Errorf("USD rate not found in ECB feed")
	}
	srcRate, ok := r.eurPer[currency]
	if !ok {
		return 0, fmt.Errorf("currency %q not found in ECB feed", currency)
	}
	return int64(math.Round((amount / srcRate) * usdRate * 100)), nil
}

func (r *Rates) RateDate() time.Time {
	return r.rateDate
}

func (r *Rates) EURPer() map[string]float64 {
	copied := make(map[string]float64, len(r.eurPer))
	for key, value := range r.eurPer {
		copied[key] = value
	}
	return copied
}
