
const LoadingOverlay = ({ percent = 0, step = "1/3", subText = "", eta = "calculating…" }) => {
  return (
    <div className="loading-overlay" aria-hidden="true">
        <div className="loading-card">
            <div className="loading-title">
                  <span id="loadingTitle">Running extraction…</span>
                  <span className="percent" id="loadingPercent">{percent}%</span>
            </div>
            <div className="progress-wrap">
          <div id="progressBar" className="progress-bar" style={{width: `${percent}%`}}></div>
            </div>
            <div className="subprogress" id="subProgressText">Step {step} • {subText}</div>
            <div className="eta" id="etaText">ETA: {eta}</div>
        </div>
    </div>
  )
}

export default LoadingOverlay