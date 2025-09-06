import React, { useEffect, useRef } from 'react';
import { createChart, CrosshairMode, LineStyle } from 'lightweight-charts';

// --- Heatmap/Volume Profile Helpers ---

const getVolumeColor = (volume, minMax) => {
    const { min, max } = minMax;
    if (max === min) return 'rgba(0, 150, 255, 0.1)';
    const ratio = (volume - min) / (max - min);
    const r = Math.round(255 * ratio);
    const g = Math.round(255 * ratio);
    const b = Math.round(255 * (1 - ratio));
    return `rgba(${r}, ${g}, ${b}, 0.5)`;
};

// A complete IPrimitive implementation for the Heatmap
class HeatmapRenderer {
    constructor(data) {
        this._data = data;
        this._renderer = null;
    }

    // --- IPrimitive interface methods ---

    attached(renderer) {
        this._renderer = renderer;
    }

    detached() {
        this._renderer = null;
    }

    update() {
        // Request a redraw on every update to keep the heatmap in sync
        if (this._renderer) {
            this._renderer.requestRedraw();
        }
    }

    draw(target) {
        target.useBitmapCoordinateSpace(scope => {
            if (scope.context === null) return;
            const ctx = scope.context;
            const { from, to } = scope.horizontalLogicalRange;

            const visibleData = this._data.filter(item => item.logicalIndex >= from && item.logicalIndex <= to);
            if (visibleData.length === 0) return;

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

                const priceStep = (item.high - item.low) / Object.keys(item.volumeProfile).length;

                Object.entries(item.volumeProfile).forEach(([priceStr, volume]) => {
                    const price = parseFloat(priceStr);
                    const y = scope.verticalPriceToCoordinate(price);
                    if (y === null) return;
                    
                    const yNext = scope.verticalPriceToCoordinate(price - priceStep);
                    const height = yNext === null ? 2 : Math.max(2, Math.abs(yNext - y));

                    ctx.fillStyle = getVolumeColor(volume, minMax);
                    ctx.fillRect(bar.x - bar.barWidth / 2, y - height / 2, bar.barWidth, height);
                });
            });
        });
    }
}

// --- Data Generation ---
function generateCandlestickData() {
    const data = [];
    let currentTime = Math.floor(Date.now() / 1000);
    let currentPrice = 100;
    
    const createCandle = (open, high, low, close, time) => {
        const volumeProfile = {};
        const numPriceLevels = 15;
        const priceRange = high - low;
        if (priceRange > 0) {
            for (let i = 0; i < numPriceLevels; i++) {
                const price = low + (priceRange * i / numPriceLevels);
                const distanceToClose = Math.abs(price - close);
                const volume = Math.random() * 100 * (1 - distanceToClose / priceRange);
                volumeProfile[price.toFixed(2)] = volume;
            }
        }
        return { time, open, high, low, close, volumeProfile };
    };

    for (let i = 0; i < 100; i++) {
        const open = currentPrice;
        let close;
        if (i > 70) { // Uptrend
            close = open + Math.random() * 2;
        } else if (i > 30) { // Sideways
            close = open + (Math.random() - 0.5) * 2;
        } else { // Downtrend
            close = open - Math.random() * 2;
        }
        const high = Math.max(open, close) + Math.random();
        const low = Math.min(open, close) - Math.random();
        data.unshift(createCandle(open, high, low, close, currentTime));
        currentPrice = close;
        currentTime -= 300;
    }
    return data;
}

// --- Indicator Calculations (unchanged) ---
function calculateRSI(data, period = 14) {
    const rsiData = [];
    let gains = 0;
    let losses = 0;
    for (let i = 1; i <= period; i++) {
        const change = data[i].close - data[i - 1].close;
        if (change > 0) gains += change; else losses -= change;
    }
    let avgGain = gains / period;
    let avgLoss = losses / period;
    rsiData.push({ time: data[period].time, value: 100 - (100 / (1 + avgGain / avgLoss)) });
    for (let i = period + 1; i < data.length; i++) {
        const change = data[i].close - data[i - 1].close;
        avgGain = (avgGain * (period - 1) + (change > 0 ? change : 0)) / period;
        avgLoss = (avgLoss * (period - 1) + (change < 0 ? -change : 0)) / period;
        const rs = avgLoss === 0 ? 100 : avgGain / avgLoss;
        rsiData.push({ time: data[i].time, value: 100 - (100 / (1 + rs)) });
    }
    return rsiData;
}
function calculateMACD(data, fastPeriod = 12, slowPeriod = 26, signalPeriod = 9) {
    const macdLine = [], signalLine = [], hist = [];
    const calculateEMA = (source, period) => {
        const ema = [source[0]];
        const multiplier = 2 / (period + 1);
        for (let i = 1; i < source.length; i++) {
            ema.push((source[i] - ema[i - 1]) * multiplier + ema[i - 1]);
        }
        return ema;
    };
    const closePrices = data.map(d => d.close);
    const emaFast = calculateEMA(closePrices, fastPeriod);
    const emaSlow = calculateEMA(closePrices, slowPeriod);
    const macdValues = [];
    for (let i = slowPeriod - 1; i < data.length; i++) {
        const macdValue = emaFast[i] - emaSlow[i];
        macdLine.push({ time: data[i].time, value: macdValue });
        macdValues.push(macdValue);
    }
    const emaSignal = calculateEMA(macdValues, signalPeriod);
    for (let i = signalPeriod - 1; i < macdLine.length; i++) {
        signalLine.push({ time: macdLine[i].time, value: emaSignal[i] });
        const histValue = macdLine[i].value - emaSignal[i];
        hist.push({ time: macdLine[i].time, value: histValue, color: histValue >= 0 ? 'rgba(0, 255, 0, 0.4)' : 'rgba(255, 0, 127, 0.4)' });
    }
    return { macdLine, signalLine, hist };
}

// --- Chart Component ---
const ChartComponent = () => {
    const chartContainerRef = useRef();

    useEffect(() => {
        const chart = createChart(chartContainerRef.current, {
            width: chartContainerRef.current.clientWidth,
            height: 700,
            layout: { background: { type: 'solid', color: '#000000' }, textColor: '#E0E0E0' },
            grid: { vertLines: { color: '#222222' }, horzLines: { color: '#222222' } },
            crosshair: { mode: CrosshairMode.Normal, vertLine: { style: LineStyle.Dashed, labelVisible: true }, horzLine: { style: LineStyle.Dashed, labelVisible: true } },
            rightPriceScale: { borderColor: '#222222' },
            timeScale: { borderColor: '#222222' },
        });

        const handleResize = () => chart.applyOptions({ width: chartContainerRef.current.clientWidth });
        window.addEventListener('resize', handleResize);

        const candleSeries = chart.addCandlestickSeries({
            upColor: '#00ff00', downColor: '#ff007f', borderVisible: false,
            wickUpColor: '#00ff00', wickDownColor: '#ff007f',
            crosshairMarkerVisible: true,
        });

        const rsiSeries = chart.addLineSeries({ color: '#00ffff', lineWidth: 2, priceScaleId: 'rsi-pane', crosshairMarkerVisible: true });
        chart.priceScale('rsi-pane').applyOptions({ height: 100, borderColor: '#222222' });

        const macdSeries = chart.addLineSeries({ color: '#ff8c00', lineWidth: 2, priceScaleId: 'macd-pane', crosshairMarkerVisible: true });
        const macdSignalSeries = chart.addLineSeries({ color: '#9d00ff', lineWidth: 2, priceScaleId: 'macd-pane', crosshairMarkerVisible: true });
        const macdHistSeries = chart.addHistogramSeries({ priceScaleId: 'macd-pane', base: 0 });
        chart.priceScale('macd-pane').applyOptions({ height: 100, borderColor: '#222222' });

        const candleData = generateCandlestickData();
        const indexedCandleData = candleData.map((item, index) => ({ ...item, logicalIndex: index }));
        const rsiData = calculateRSI(candleData);
        const { macdLine, signalLine, hist } = calculateMACD(candleData);

        candleSeries.setData(indexedCandleData);
        rsiSeries.setData(rsiData);
        macdSeries.setData(macdLine);
        macdSignalSeries.setData(signalLine);
        macdHistSeries.setData(hist);

        const heatmapRenderer = new HeatmapRenderer(indexedCandleData);
        candleSeries.attachPrimitive(heatmapRenderer);
        
        chart.timeScale().fitContent();

        return () => { window.removeEventListener('resize', handleResize); chart.remove(); };
    }, []);

    return <div ref={chartContainerRef} />;
};

export default ChartComponent;