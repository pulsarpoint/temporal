package fxrates

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const ecbFixture = `<?xml version="1.0" encoding="UTF-8"?>
<gesmes:Envelope xmlns:gesmes="http://www.gesmes.org/xml/2002-08-01" xmlns="http://www.ecb.int/vocabulary/2002-08-01/eurofxref">
  <Cube>
    <Cube time="2026-05-20">
      <Cube currency="USD" rate="1.0900"/>
      <Cube currency="NOK" rate="11.5000"/>
    </Cube>
    <Cube time="2026-05-17">
      <Cube currency="USD" rate="1.0800"/>
      <Cube currency="NOK" rate="11.4000"/>
    </Cube>
  </Cube>
</gesmes:Envelope>`

func TestParseForDateUsesLatestOfficialRateOnOrBeforeDate(t *testing.T) {
	requestedDate, err := time.Parse("2006-01-02", "2026-05-19")
	require.NoError(t, err)

	rates, err := parseForDate(strings.NewReader(ecbFixture), requestedDate)
	require.NoError(t, err)

	require.Equal(t, "2026-05-17", rates.RateDate().Format("2006-01-02"))
	require.Equal(t, float64(1.08), rates.EURPer()["USD"])
	require.Equal(t, float64(11.4), rates.EURPer()["NOK"])
}

func TestParseLatestUsesFirstRateDay(t *testing.T) {
	rates, err := parse(strings.NewReader(ecbFixture))
	require.NoError(t, err)

	require.Equal(t, "2026-05-20", rates.RateDate().Format("2006-01-02"))
}
