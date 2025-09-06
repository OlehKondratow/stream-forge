import React, { useEffect, useRef, useState } from 'react';
import { createChart } from 'lightweight-charts';
import axios from 'axios';

// --- Helper Functions ---

// Function to determine color based on volume (simple linear scale)
const getVolumeColor = (volume, minMax) => {
  const { min, max } = minMax;
  if (max === min) return 'rgba(0, 150, 255, 0.1)'; // Default color if all volumes are the same

  const ratio = (volume - min) / (max - min);
  const blue = 0;
  const green = Math.round(255 * ratio);
  const red = Math.round(255 * (1 - ratio));

  return `rgba(${red}, ${green}, ${blue}, 0.2)`; // Use some transparency
};

// Custom Pane Renderer for the Heatmap
class HeatmapRenderer {
  constructor(data) {
    this._data = data;
  }

  draw(target) {
    target.useBitmapCoordinateSpace(scope => {
      const ctx = scope.context;
      const { from, to } = scope.horizontalLogicalRange;

      const visibleData = this._data.filter(item => item.logicalIndex >= from && item.logicalIndex <= to);

      if (visibleData.length === 0) return;

      // Find min/max volume in the visible range for color scaling
      let minVolume = Infinity;
      let maxVolume = -Infinity;
      visibleData.forEach(item => {
        if (item.volumeProfile) {
          Object.values(item.volumeProfile).forEach(vol => {
            if (vol < minVolume) minVolume = vol;
            if (vol > maxVolume) maxVolume = vol;
          });
        }
      });

      const minMax = { min: minVolume, max: maxVolume };

      visibleData.forEach(item => {
        if (!item.volumeProfile) return;

        const bar = scope.bars.at(item.logicalIndex);
        if (bar === null) return;

        Object.entries(item.volumeProfile).forEach(([priceStr, volume]) => {
          const price = parseFloat(priceStr);
          const y = scope.verticalPriceToCoordinate(price);
          
          // We need to find the y for the next price step to draw a rectangle
          // This is a simplification: we draw a small, fixed-height rectangle
          const yNext = scope.verticalPriceToCoordinate(price - 0.00001); // Assuming a small step
          const height = Math.abs(yNext - y);

          ctx.fillStyle = getVolumeColor(volume, minMax);
          ctx.fillRect(bar.x - bar.barWidth / 2, y - height / 2, bar.barWidth, height);
        });
      });
    });
  }
}

const ChartComponent = () => {
  const chartContainerRef = useRef();
  const chartRef = useRef();
  const [data, setData] = useState([]);

  useEffect(() => {
    axios.get('http://localhost:8000/api/data?symbol=PIXELUSDT')
      .then(response => {
        const formattedData = response.data.map(item => ({
          time: item.timestamp / 1000, // Lightweight Charts expects seconds
          open: item.candle.open,
          high: item.candle.high,
          low: item.candle.low,
          close: item.candle.close,
          volume: item.candle.volume,
          rsi: item.indicators.rsi_14,
          macd: item.indicators.MACD_12_26_9,
          macd_signal: item.indicators.MACDs_12_26_9,
          macd_hist: item.indicators.MACDh_12_26_9,
          volumeProfile: item.volume_profile,
        }));
        setData(formattedData);
      })
      .catch(error => console.error("Error fetching data:", error));
  }, []);

  useEffect(() => {
    if (data.length === 0) return;

    const handleResize = () => {
      chartRef.current.applyOptions({ width: chartContainerRef.current.clientWidth });
    };

    chartRef.current = createChart(chartContainerRef.current, {
      width: chartContainerRef.current.clientWidth,
      height: 600,
      layout: {
        background: { color: '#121212' },
        textColor: '#ffffff',
      },
      grid: {
        vertLines: { color: '#333' },
        horzLines: { color: '#333' },
      },
      crosshair: {
        mode: 'normal',
      },
    });

    // --- Main Pane: Candles and Heatmap ---
    const candleSeries = chartRef.current.addCandlestickSeries({
      upColor: '#26a69a',
      downColor: '#ef5350',
      borderDownColor: '#ef5350',
      borderUpColor: '#26a69a',
      wickDownColor: '#ef5350',
      wickUpColor: '#26a69a',
    });
    candleSeries.setData(data);

    // Add custom heatmap renderer
    const heatmapData = data.map((item, index) => ({ ...item, logicalIndex: index }));
    const heatmapRenderer = new HeatmapRenderer(heatmapData);
    candleSeries.attachPrimitive(heatmapRenderer);

    // --- Volume Pane ---
    const volumeSeries = chartRef.current.addHistogramSeries({
      color: '#26a69a',
      priceFormat: {
        type: 'volume',
      },
      priceScaleId: '', // Set to an empty string to display the volume scale on the left
    });
    volumeSeries.setData(data.map(item => ({ time: item.time, value: item.volume, color: item.close > item.open ? 'rgba(38, 166, 154, 0.5)' : 'rgba(239, 83, 80, 0.5)' })));

    // --- RSI Pane ---
    const rsiPane = chartRef.current.addPriceScale('rsi', { scaleMargins: { top: 0.8, bottom: 0 } });
    const rsiSeries = chartRef.current.addLineSeries({
      priceScaleId: 'rsi',
      color: '#ffc107',
      lineWidth: 2,
    });
    rsiSeries.setData(data.map(item => ({ time: item.time, value: item.rsi })));

    // --- MACD Pane ---
    const macdPane = chartRef.current.addPriceScale('macd', { scaleMargins: { top: 0.8, bottom: 0 } });
    const macdSeries = chartRef.current.addLineSeries({ priceScaleId: 'macd', color: '#2196f3', lineWidth: 2 });
    const macdSignalSeries = chartRef.current.addLineSeries({ priceScaleId: 'macd', color: '#f44336', lineWidth: 2 });
    const macdHistSeries = chartRef.current.addHistogramSeries({ priceScaleId: 'macd', color: '#9c27b0' });
    macdSeries.setData(data.map(item => ({ time: item.time, value: item.macd })))
    macdSignalSeries.setData(data.map(item => ({ time: item.time, value: item.macd_signal })))
    macdHistSeries.setData(data.map(item => ({ time: item.time, value: item.macd_hist, color: item.macd_hist > 0 ? 'rgba(38, 166, 154, 0.5)' : 'rgba(239, 83, 80, 0.5)' })))

    window.addEventListener('resize', handleResize);

    return () => {
      window.removeEventListener('resize', handleResize);
      chartRef.current.remove();
    };
  }, [data]);

  return <div ref={chartContainerRef} style={{ position: 'relative' }} />;
};

export default ChartComponent;
