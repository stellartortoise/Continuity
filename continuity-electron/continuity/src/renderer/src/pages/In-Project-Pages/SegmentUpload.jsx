import { useState, useEffect } from 'react'
import { uploadSegment, reviewFact, submitReviewSession } from '../../services/aiService';
import EntityAnalysisCard from '../../components/ProjectLayout/EntityAnalysisCard';
import LoadingOverlay from '../../components/ProjectLayout/LoadingOverlay';
import projectService from '../../services/projectService';

const SegmentUpload = ({setSegments}) => {
    const [segment,setSegment] = useState("");
    const [analysis, setAnalysis] = useState(null);
    const [projectId, setProjectId] = useState(null);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);
    const [segmentTitle, setSegmentTitle] = useState("");
    const [submitMessage, setSubmitMessage] = useState(null);
    const [isSubmittingReview, setIsSubmittingReview] = useState(false);
    const [progress, setProgress] = useState(0);
    const [step, setStep] = useState("1/3");
    const [subText, setSubText] = useState("");
    const [eta, setEta] = useState("calculating…");

    useEffect(() => {
        const loadProject = async () => {
            const project = await projectService.loadProject();
            setProjectId(project?.id || null);
        };
        loadProject();
    }, []);

    useEffect(() => {
        if (!loading) return;

        const start = Date.now();

        setProgress(0);
        setStep("1/3");
        setSubText("Extracting entities…");

        const interval = setInterval(() => {
            setProgress(prev => {
                const next = prev + 5;

                if (next >= 30) {
                    setStep("2/3");
                    setSubText("Aggregating facts…");
                }

                if (next >= 70) {
                    setStep("3/3");
                    setSubText("Rendering results…");
                }

                const elapsed = (Date.now() - start) / 1000;

                if (elapsed > 1) {
                    const rate = next / elapsed; // % per second

                    if (rate > 0) {
                        const remaining = 100 - next;
                        const etaSeconds = remaining / rate;

                        const mm = String(Math.floor(etaSeconds / 60)).padStart(2, "0");
                        const ss = String(Math.floor(etaSeconds % 60)).padStart(2, "0");

                        setEta(`${mm}:${ss}`);
                    }
                }

                if (next >= 90) return prev;

                return next;
            });
        }, 300);

        return () => clearInterval(interval);
    }, [loading]);


    const handleSubmit = async(e) => {
        e.preventDefault();

        if (!segment.trim()) return;
        if (!projectId) {
            setError("No active project selected.");
            return;
        }

        setError(null);
        setSubmitMessage(null);
        setLoading(true); // show the loading overlayyy

        try {
            //load backend
            const result = await uploadSegment(projectId, segment, segmentTitle.trim() || undefined);

            const newSegment = {
                id: result.story?.id || Date.now(),
                title: result.story?.title || segmentTitle.trim() || `Segment ${Date.now()}`,
                summary:result.summary,
                text:segment,
                entities:result.entities,
                entitySummary: result.entitySummary || [],
                reviewSessionId: result.reviewSessionId,
                pendingFactsCount: result.pendingFactsCount,
                conflictsDetected: result.conflictsDetected,
                crossStoryConflictsCount: result.crossStoryConflictsCount || 0,
            }

            // Save it to global state
            setSegments(prev => [...prev, newSegment]);

            // Attach segmentId to analysis
            setAnalysis({
                ...result,
                segmentId: newSegment.id
            });

        } catch(error){
            console.error(error);
            setError(error.message || "Failed to upload segment");
        } finally {
            setProgress(100);
            setStep("3/3");
            setSubText("Done!");

            setTimeout(() => {
                setSegmentTitle("");
                setSegment("");
                setLoading(false);
            }, 400); // hide the overlay
        }


    }

    const updateFactLocally = (entityId, factId, accepted) => {
        setAnalysis(prev => {
            if (!prev) return prev;
            return {
                ...prev,
                entities: prev.entities.map(e => {
                    if (e.id !== entityId) return e;
                    return {
                        ...e,
                        facts: e.facts.map(f => (f.id === factId ? { ...f, accepted } : f)),
                    };
                }),
            };
        });

        setSegments(prev =>
            prev.map(seg => {
                if (seg.id !== analysis?.segmentId) return seg;
                return {
                    ...seg,
                    entities: seg.entities.map(e => {
                        if (e.id !== entityId) return e;
                        return {
                            ...e,
                            facts: e.facts.map(f => (f.id === factId ? { ...f, accepted } : f)),
                        };
                    }),
                };
            })
        );
    };

    const handleAccept = async (entityId, factId) => {
        setError(null);
        try {
            const currentFact = (analysis?.entities || [])
                .flatMap((entity) => entity.facts || [])
                .find((fact) => fact.id === factId);
            const isLowQuality = Boolean(
                currentFact && (
                    currentFact.needsReview ||
                    (typeof currentFact.atomicityScore === "number" && currentFact.atomicityScore < 0.75) ||
                    (typeof currentFact.schemaAlignmentScore === "number" && currentFact.schemaAlignmentScore < 0.55)
                )
            );
            await reviewFact(
                factId,
                "approved",
                null,
                isLowQuality ? "User approved low-quality fact" : null,
                true,
                isLowQuality,
            );
            updateFactLocally(entityId, factId, true);
        } catch (e) {
            setError(e.message || "Failed to approve fact");
        }
    };

    const handleReject = async (entityId, factId) => {
        setError(null);
        try {
            await reviewFact(factId, "rejected", null, null, true);
            updateFactLocally(entityId, factId, false);
        } catch (e) {
            setError(e.message || "Failed to reject fact");
        }
    };

    const countPendingFacts = () => {
        return (analysis?.entities || []).reduce((count, entity) => {
            return count + (entity.facts || []).filter(f => f.accepted === null).length;
        }, 0);
    };

    const handleSubmitReview = async () => {
        if (!analysis?.reviewSessionId) return;
        setError(null);
        setSubmitMessage(null);
        setIsSubmittingReview(true);
        try {
            const result = await submitReviewSession(analysis.reviewSessionId);
            setSubmitMessage(result.message || "Review submitted");
        } catch (e) {
            setError(e.message || "Failed to submit review session");
        } finally {
            setIsSubmittingReview(false);
        }
    };

    
  return (
    <>
    {/* loading overlay appears if segment is loading */}
          {loading && <LoadingOverlay
              percent={progress}
              step={step}
              subText={subText}
              eta={eta}
          />}

    <form className='segmentUpload' onSubmit={handleSubmit}>
        <label htmlFor='userSegment'>Submit your Story Segment:</label>

        <label htmlFor='segmentTitle'>Segment Title:</label>
        <input id='segmentTitle' type='text' value={segmentTitle} onChange={(e)=> setSegmentTitle(e.target.value)} placeholder='Ex: Chapter 1' maxLength={150}/>

        <textarea id='userSegment' 
                value={segment} 
                onChange={(e)=> setSegment(e.target.value)} 
                rows={10} 
                placeholder='Paste or type your text here...'
                maxLength={30000}
                />

        <div className='form-footer'>
            <div className='characterLength'>
                {segment.length} characters
            </div>
            <div>
                <button type='submit' disabled={!segment.trim() || !projectId || loading}>
                    Submit
                </button>
            </div>
        </div>
    </form>

    {error && <p style={{color: "#a33"}}>{error}</p>}
    {submitMessage && <p style={{color: "#2a6"}}>{submitMessage}</p>}

    {/* Only render below if analysis exists */}
        {analysis && (
            <div className='segment-summary' style={{padding:"2rem", backgroundColor:"red",margin:"5px", borderRadius:"8px" }}>
                <div>{analysis.summary}</div>
                <div style={{marginTop: "0.5rem"}}>
                    Pending facts: {analysis.pendingFactsCount || 0} | Conflicts: {analysis.conflictsDetected || 0}
                    {` | Cross-story conflicts: ${analysis.crossStoryConflictsCount || 0}`}
                </div>
                {analysis.entitySummary?.length > 0 && (
                    <div style={{marginTop: "0.75rem", background: "rgba(255,255,255,0.08)", padding: "0.5rem", borderRadius: "6px"}}>
                        <strong>Extraction Summary</strong>
                        {analysis.entitySummary.map((entity) => (
                            <div key={entity.entityId || entity.entityName} style={{marginTop: "0.35rem"}}>
                                <div>{entity.entityName} ({entity.entityType})</div>
                                {(entity.sections || []).map((section) => (
                                    <div key={`${entity.entityId}-${section.name}`} style={{opacity: 0.9}}>
                                        {section.name}: {(section.facts || []).join(" | ")}
                                    </div>
                                ))}
                            </div>
                        ))}
                    </div>
                )}
                <div style={{marginTop: "0.75rem"}}>
                    <button
                        type='button'
                        onClick={handleSubmitReview}
                        disabled={isSubmittingReview || countPendingFacts() > 0 || !analysis.reviewSessionId}
                    >
                        {isSubmittingReview ? "Submitting..." : "Submit Canon Decisions"}
                    </button>
                    {countPendingFacts() > 0 && <span style={{marginLeft: "0.5rem"}}>Resolve all facts before submit.</span>}
                </div>
            </div>
        )}
        {analysis && analysis.entities.map(entity => (
            <EntityAnalysisCard
                key={entity.id}
                entity={entity}
                entityOptions={analysis.entities}
                editable={true}
                onAccept={handleAccept}
                onReject={handleReject}
            />
        ))}
    </>
  )
}

export default SegmentUpload