

const LoadingOverlay = ({ percent = 0, step = "1/3", subText = "", eta = "calculating…" }) => {

  return (
    <div className="loading-overlay" aria-hidden={!percent}>
        <div className="loading-card">
            <div className="loading-title">
                  <span>Running extraction…</span>
                  <span className="percent">{percent}%</span>
            </div>

            <div className="progress-wrap">
            
            <div className="progress-bar" style={{width: `${percent}%`}}></div>

            </div>

            <div className="subprogress" id="subProgressText">
              Step {step} • {subText}
            </div>

            <div className="eta" id="etaText">
              ETA: {eta}
            </div>

            
            <div className="loading-foot" id="loadingFoot">
              This may take a moment for longer texts.
            </div>
        </div>
    </div>
  )
}

export default LoadingOverlay