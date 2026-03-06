
const LoadingOverlay = () => {
  return (
    <div className="loading-overlay" aria-hidden="true">
        <div className="loading-card">
            <div className="loading-title">
                  <span id="loadingTitle">Running extraction…</span>
                  <span className="percent" id="loadingPercent">0%</span>
            </div>
            <div className="progress-wrap">
                <div id="progressBar" className="progress-bar"></div>
            </div>
            <div className="subprogress" id="subProgressText">Step 1/3 • NER progress: 0% (overall 0%)</div>
            <div className="eta" id="etaText">ETA: calculating…</div>
        </div>
    </div>
  )
}

export default LoadingOverlay