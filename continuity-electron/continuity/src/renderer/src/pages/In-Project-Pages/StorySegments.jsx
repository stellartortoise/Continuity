import { useState, useEffect } from "react"
import SegmentTextBlock from "../../components/ProjectLayout/SegmentTextBlock";
import EntityAnalysisCard from "../../components/ProjectLayout/EntityAnalysisCard";
import projectService from "../../services/projectService";
import { reviewFact, submitReviewSession } from "../../services/aiService";

const API_BASE = "http://localhost:8001";


const StorySegments = ({uploadedSegments}) => {
  const [openId, setOpenId] = useState(null);
  const [storedSegments, setStoredSegments] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [submitMessages, setSubmitMessages] = useState({});
  const [submittingBySegment, setSubmittingBySegment] = useState({});

  useEffect(() => {
    const loadStored = async () => {
      setLoading(true);
      setError(null);
      try {
        const project = await projectService.loadProject();
        if (!project?.id) {
          setStoredSegments([]);
          return;
        }

        const storyRes = await fetch(`${API_BASE}/stories?project_id=${project.id}`);
        if (!storyRes.ok) {
          setStoredSegments([]);
          return;
        }

        const stories = await storyRes.json();
        const hydrated = await Promise.all(
          (stories || []).map(async (s) => {
            const entRes = await fetch(`${API_BASE}/stories/${s.id}/entities`);
            const entData = entRes.ok ? await entRes.json() : { entities: [] };
            const entities = (entData.entities || []).map((entity) => ({
              id: entity.id,
              name: entity.name,
              type: entity.entityType,
              aliases: entity.aliases || [],
              facts: (entity.facts || []).map((fact) => ({
                id: fact.id,
                text: fact.fact,
                accepted: fact.status === "approved" ? true : fact.status === "rejected" ? false : null,
              })),
            }));

            return {
              id: s.id,
              title: s.title,
              summary: (s.body || "").slice(0, 140) + ((s.body || "").length > 140 ? "..." : ""),
              text: s.body,
              entities,
              reviewSessionId: s.reviewSessionId,
              pendingFactsCount: s.pendingFactsCount,
              conflictsDetected: s.conflictsDetected,
            };
          })
        );
        setStoredSegments(hydrated);
      } catch (e) {
        console.error(e);
        setError(e.message || "Failed to load saved segments");
        setStoredSegments([]);
      } finally {
        setLoading(false);
      }
    };

    loadStored();
  }, [uploadedSegments]);

  const segments = storedSegments.length > 0 ? storedSegments : uploadedSegments;


  const toggleCard = (id) => {
    setOpenId(prev => (prev=== id ? null : id));
  }

  const updateFactDecision = (segmentId, entityId, factId, accepted) => {
    setStoredSegments(prev => prev.map(seg => {
      if (seg.id !== segmentId) return seg;
      return {
        ...seg,
        entities: seg.entities.map(ent => {
          if (ent.id !== entityId) return ent;
          return {
            ...ent,
            facts: ent.facts.map(f => f.id === factId ? { ...f, accepted } : f),
          };
        }),
      };
    }));
  };

  const getPendingCount = (segment) => {
    return (segment.entities || []).reduce((count, ent) => {
      return count + (ent.facts || []).filter(f => f.accepted === null).length;
    }, 0);
  };

  const handleAccept = async (segmentId, entityId, factId) => {
    setError(null);
    try {
      await reviewFact(factId, "approved");
      updateFactDecision(segmentId, entityId, factId, true);
    } catch (e) {
      setError(e.message || "Failed to approve fact");
    }
  };

  const handleReject = async (segmentId, entityId, factId) => {
    setError(null);
    try {
      await reviewFact(factId, "rejected");
      updateFactDecision(segmentId, entityId, factId, false);
    } catch (e) {
      setError(e.message || "Failed to reject fact");
    }
  };

  const handleSubmitSession = async (segment) => {
    if (!segment.reviewSessionId) return;
    setError(null);
    setSubmitMessages(prev => ({ ...prev, [segment.id]: null }));
    setSubmittingBySegment(prev => ({ ...prev, [segment.id]: true }));
    try {
      const result = await submitReviewSession(segment.reviewSessionId);
      setSubmitMessages(prev => ({ ...prev, [segment.id]: result.message || "Review submitted" }));
    } catch (e) {
      setError(e.message || "Failed to submit review session");
    } finally {
      setSubmittingBySegment(prev => ({ ...prev, [segment.id]: false }));
    }
  };

  return (
    <div className='story-segments'>
      <h1>My Story Segments</h1>
      {loading && <p>Loading saved segments...</p>}
      {error && <p style={{color: "#a33"}}>{error}</p>}
      
      {segments.map(segment => (
        <div 
            key={segment.id} 
            className="segment-card" 
            onClick={()=> toggleCard(segment.id)} 
          style={{
            cursor: "pointer", marginBottom: "1rem", border: "1px solid #ccc", borderRadius: "8px", padding: "0.5rem"}}>

          <div className="segment-card-header">
            <h2>{segment.title} {openId === segment.id ? "▼" : "▶"}</h2>
          </div>

          <div className="segment-card-body">
            <p>{segment.summary}</p>
            <p>Pending facts: {getPendingCount(segment)} | Conflicts: {segment.conflictsDetected || 0}</p>
            {submitMessages[segment.id] && <p style={{color: "#2a6"}}>{submitMessages[segment.id]}</p>}
          </div>

          {openId === segment.id && (
            <div className="accordion-body open" style={{
                        display: "flex",
                        flexDirection: "column",
                        gap: "1rem",
                        marginTop: "1rem",
                        maxHeight: "400px",
                        overflowY: "auto",  
                        paddingRight: "0.5rem"
}}>
              {/* THE FULL TEXT */}
              <SegmentTextBlock segment={segment.text}/>

              <div>
                <button
                  type="button"
                  onClick={(e) => {
                    e.stopPropagation();
                    handleSubmitSession(segment);
                  }}
                  disabled={submittingBySegment[segment.id] || getPendingCount(segment) > 0 || !segment.reviewSessionId}
                >
                  {submittingBySegment[segment.id] ? "Submitting..." : "Submit Canon Decisions"}
                </button>
                {getPendingCount(segment) > 0 && <span style={{marginLeft: "0.5rem"}}>Resolve all facts before submit.</span>}
              </div>

              {/* Entity cards - review mode */}
              <div style={{ flexShrink: 0, minWidth: "250px" }}>
              {segment.entities.map(entity =>(
                <EntityAnalysisCard 
                  key={entity.id} 
                  entity={entity} 
                  editable={true}
                  onAccept={(entityId, factId) => handleAccept(segment.id, entityId, factId)}
                  onReject={(entityId, factId) => handleReject(segment.id, entityId, factId)}
                  />
                ))}
              </div>
            </div>
          )}

        </div>
      ))}

    </div>
  )
}

export default StorySegments