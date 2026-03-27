
const EntityAnalysisCard = ({ entity, editable = false, onAccept, onReject, onResolveConflict }) => {
  const facts = entity.facts || [];
  const conflictFacts = facts.filter((fact) => fact.conflictGroupId || (fact.contradicts || []).length > 0);
  const normalFacts = facts.filter((fact) => !fact.conflictGroupId && (fact.contradicts || []).length === 0);

  const conflictGroups = conflictFacts.reduce((acc, fact) => {
    const groupId = fact.conflictGroupId || `pair-${fact.id}`;
    if (!acc[groupId]) {
      acc[groupId] = [];
    }
    acc[groupId].push(fact);
    return acc;
  }, {});

  const getFactDecisionLabel = (fact) => {
    if (fact.accepted === true) return "Canon selected";
    if (fact.accepted === false) return "Rejected";
    return "Pending choice";
  };

  return (
    <div className="entity-card">
        <h3>{entity.name}</h3>
        <div>Type: {entity.type}</div>
        <div>Aliases: {entity.aliases.length > 0 ? entity.aliases.join(","): "None"}</div>

        {Object.keys(conflictGroups).length > 0 && (
          <div className="conflict-zone">
            <h3>Conflict Resolution Required</h3>
            <p>Choose one fact in each red conflict box as canon. Opposing facts are auto-rejected.</p>

            {Object.entries(conflictGroups).map(([groupId, groupFacts]) => (
              <div key={groupId} className="conflict-group">
                <div className="conflict-group-header">Conflict Group {groupId}</div>
                <ul className="conflict-options">
                  {groupFacts.map((fact) => (
                    <li key={fact.id} className="conflict-option">
                      <div className="fact-text-row">
                        <span className="fact-text">{fact.text}</span>
                        <span className={`decision-pill decision-${fact.accepted === true ? 'approved' : fact.accepted === false ? 'rejected' : 'pending'}`}>
                          {getFactDecisionLabel(fact)}
                        </span>
                      </div>

                      {(fact.contradicts || []).length > 0 && (
                        <div className="fact-conflict-meta">Conflicts with: {(fact.contradicts || []).join(', ')}</div>
                      )}

                      {editable && (
                        <div className="conflict-actions">
                          <button
                            type="button"
                            className="resolve-canon-btn"
                            onClick={() => onResolveConflict(entity.id, fact.id)}
                          >
                            Set As Canonical Truth
                          </button>
                        </div>
                      )}
                    </li>
                  ))}
                </ul>
              </div>
            ))}
          </div>
        )}

        <h3>Facts</h3>
        <ul className="fact-list">
          {normalFacts.map((fact) => (
            <li key={fact.id} className={`fact fact-${fact.accepted} ${(fact.conflictGroupId || (fact.contradicts || []).length > 0) ? 'fact-conflict' : ''}`}>
              <div className="fact-text-row">
                <span className="fact-text">{fact.text}</span>
                {(fact.conflictGroupId || (fact.contradicts || []).length > 0) && (
                  <span className="fact-conflict-badge">Conflict</span>
                )}
              </div>

              {(fact.conflictGroupId || (fact.contradicts || []).length > 0) && (
                <div className="fact-conflict-meta">
                  {fact.conflictGroupId && <span>Group: {fact.conflictGroupId}</span>}
                  {(fact.contradicts || []).length > 0 && <span>Conflicts with: {(fact.contradicts || []).join(', ')}</span>}
                </div>
              )}

              {editable && (
                <span className="fact-actions">
                  <button onClick={() => onAccept(entity.id, fact.id)} style={{
                    background: fact.accepted === true ? "green" : "#ccc",
                    color: fact.accepted === true ? "white" : "black"
                  }}>Accept</button>
                  <button onClick={() => onReject(entity.id, fact.id)} style={{
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
          ))}
        </ul>

    </div>
  )
}

export default EntityAnalysisCard