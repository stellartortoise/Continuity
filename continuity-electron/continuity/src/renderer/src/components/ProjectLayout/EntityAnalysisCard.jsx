
const EntityAnalysisCard = ({
  entity,
  entityOptions = [],
  selectedEntityByFact = {},
  editable = false,
  onAccept,
  onReject,
  onChangeEntity,
}) => {
  const visibleFacts = entity.facts || [];

  return (
    <div className="entity-card">
        <h3>{entity.name}</h3>
        <div>Type: {entity.type}</div>
        <div>Aliases: {entity.aliases.length > 0 ? entity.aliases.join(","): "None"}</div>

        <h3>Facts</h3>
        <ul className="fact-list">
          {visibleFacts.map((fact) => {
            const requiresExplicitSelection = Boolean(fact.matchAmbiguous && !fact.assignmentConfirmed);
            const selectedEntityId =
              selectedEntityByFact[fact.id] ??
              (requiresExplicitSelection ? "" : (fact.entityId || entity.id));
            const reviewBlocked = requiresExplicitSelection && !selectedEntityId;

            return (
            <li key={fact.id} className={`fact fact-${fact.accepted}`}>
              {fact.text}
              {typeof fact.matchConfidence === "number" && fact.matchConfidence > 0 ? (
                <span style={{ marginLeft: "0.5rem", opacity: 0.75 }}>
                  [{Math.round(fact.matchConfidence * 100)}% match]
                </span>
              ) : null}
              {fact.matchAmbiguous ? (
                <span style={{ marginLeft: "0.5rem", color: "#b55" }}>
                  Ambiguous entity match
                </span>
              ) : null}
              {fact.needsReview ? (
                <span style={{ marginLeft: "0.5rem", color: "#d68c1f" }}>
                  Needs quality review
                </span>
              ) : null}

              {(typeof fact.atomicityScore === "number" || typeof fact.schemaAlignmentScore === "number") ? (
                <span style={{ display: "block", marginTop: "0.2rem", opacity: 0.8 }}>
                  {typeof fact.atomicityScore === "number" ? `Atomicity: ${Math.round(fact.atomicityScore * 100)}%` : null}
                  {typeof fact.schemaAlignmentScore === "number" ? ` | Schema match: ${Math.round(fact.schemaAlignmentScore * 100)}%` : null}
                </span>
              ) : null}

              {fact.matchCandidates?.length ? (
                <span style={{ display: "block", marginTop: "0.35rem", opacity: 0.8 }}>
                  Candidates: {fact.matchCandidates.map((item) => `${item.entity_name} (${Math.round((item.score || 0) * 100)}%)`).join(" | ")}
                </span>
              ) : null}

              {editable && entityOptions.length > 0 && (
                <span className="fact-entity-override" style={{ display: "inline-flex", marginLeft: "0.75rem" }}>
                  <select
                    value={selectedEntityId}
                    onChange={(event) => onChangeEntity?.(entity.id, fact.id, event.target.value)}
                  >
                    {requiresExplicitSelection && <option value="">Select entity before review</option>}
                    {entityOptions.map((option) => (
                      <option key={option.id} value={option.id}>
                        {option.name}
                      </option>
                    ))}
                  </select>
                </span>
              )}

              {reviewBlocked ? (
                <span style={{ marginLeft: "0.5rem", color: "#b55" }}>
                  Select an entity to continue.
                </span>
              ) : null}

              {editable && (
                <span className="fact-actions">
                  <button disabled={reviewBlocked} onClick={() => onAccept(entity.id, fact.id)} style={{
                    background: fact.accepted === true ? "green" : "#ccc",
                    color: fact.accepted === true ? "white" : "black"
                  }}>Accept</button>
                  <button disabled={reviewBlocked} onClick={() => onReject(entity.id, fact.id)} style={{
                    background: fact.accepted === false ? "red" : "#ccc",
                    color: fact.accepted === false ? "white" : "black"
                  }}>Reject</button>
                </span>
              )}

              {!editable && (
                <span className="fact-status">
                  {fact.accepted === true && "✓ accepted"}
                  {fact.accepted === false && "✕ rejected"}
                  {fact.accepted === null && "• undecided"}
                </span>
              )}
            </li>
          )})}
        </ul>

    </div>
  )
}

export default EntityAnalysisCard