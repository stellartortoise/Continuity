import { useState, useEffect } from 'react'
import { getSegmentExtractionResult, getSegmentExtractionStatus, reviewFact, startSegmentExtraction, submitReviewSession } from '../../services/aiService';
import EntityAnalysisCard from '../../components/ProjectLayout/EntityAnalysisCard';
import LoadingOverlay from '../../components/ProjectLayout/LoadingOverlay';

const SegmentUpload = ({setSegments}) => {
    const [segment,setSegment] = useState("");
    const [analysis, setAnalysis] = useState(null);
    const [loading, setLoading] = useState(false);
    const [segmentTitle, setSegmentTitle] = useState("");
    const [error, setError] = useState(null);
    const [submitError, setSubmitError] = useState(null);
    const [submitMessage, setSubmitMessage] = useState(null);
    const [isSubmittingReview, setIsSubmittingReview] = useState(false);
    const [projectId, setProjectId] = useState(null);

    useEffect(() => {
        const pid = localStorage.getItem("currentProjectId");
        setProjectId(pid);
    }, []);
    const [loadingProgress, setLoadingProgress] = useState({
        percent:0,
        step: "1/3",
        subText: "Preparing extraction",
        eta: "calculating…"
    })

    const wait = (ms) => new Promise(resolve => setTimeout(resolve, ms));

    const stepFromPhase = (phase) => {
        const map = {
            ner: "1/3",
            facts: "2/3",
            fact_extraction: "2/3",
            validate: "3/3",
            finalize: "3/3",
            complete: "3/3",
            working: "2/3",
        };
        return map[phase] || "2/3";
    };

    const buildSubText = (status) => {
        const processed = Number(status?.processed || 0);
        const total = Number(status?.total || 0);
        const entityName = status?.currentEntityName;
        const message = status?.message || "Running extraction";

        if (entityName && total > 0) {
            return `${message} (${processed}/${total}) - ${entityName}`;
        }

        if (total > 0) {
            return `${message} (${processed}/${total})`;
        }

        return message;
    };

    
    const handleSubmit = async(e) => {
        e.preventDefault();

        if (!segment.trim()) return;
        if (!projectId) {
            setError("Project ID not loaded");
            return;
        }

        setError(null);
        setSubmitError(null);
        setSubmitMessage(null);
        setLoadingProgress({
            percent: 3,
            step: "1/3",
            subText: "Creating segment and queuing extraction",
            eta: "calculating…"
        });
        setLoading(true); // show the loading overlayyy

        try {
            const startTime = Date.now();
            const started = await startSegmentExtraction(projectId, segment, segmentTitle);
            const jobId = started.jobId;

            let statusPayload = null;
            for (let i = 0; i < 240; i += 1) {
                statusPayload = await getSegmentExtractionStatus(jobId);
                const progress = Math.max(0, Math.min(1, Number(statusPayload.progress || 0)));
                const percent = Math.max(5, Math.min(98, Math.round(progress * 100)));

                const elapsedSec = Math.max(1, Math.floor((Date.now() - startTime) / 1000));
                const estimatedTotal = progress > 0 ? Math.round(elapsedSec / progress) : null;
                const remaining = estimatedTotal ? Math.max(0, estimatedTotal - elapsedSec) : null;

                setLoadingProgress({
                    percent,
                    step: stepFromPhase(statusPayload.phase),
                    subText: buildSubText(statusPayload),
                    eta: remaining === null ? "calculating…" : `${remaining}s remaining`,
                });

                if (statusPayload.status === "done") {
                    break;
                }

                if (statusPayload.status === "error") {
                    throw new Error(statusPayload.message || "Entity extraction failed");
                }

                await wait(650);
            }

            setLoadingProgress({
                percent: 99,
                step: "3/3",
                subText: "Finalizing entities and conflicts",
                eta: "a few seconds",
            });

            const result = await getSegmentExtractionResult(jobId, segment);

            setLoadingProgress({
                percent: 100,
                step: "3/3",
                subText: "Extraction complete",
                eta: "done",
            });

            const newSegment = {
                id: Date.now(),
                title: segmentTitle.trim() || `Segment ${Date.now()}`, // inputted title or fallback title
                summary:result.summary,
                text:segment,
                entities:result.entities,
                reviewSessionId: result.reviewSessionId
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
            setSegmentTitle("");
            setSegment("");
            setLoading(false); // hide the overlay
        }


    }

    const updateFactDecisionLocal = (entityId, factId, accepted) => {
        const nextStatus = accepted ? "approved" : "rejected";

        setAnalysis(prev => ({
            ...prev,
            entities: prev.entities.map(e => {
                if (e.id !== entityId) return e;

                return {
                    ...e,
                    facts: e.facts.map(f =>
                        f.id === factId ? { ...f, accepted, status: nextStatus } : f
                    )
                };
            })
        }));

        setSegments(prev =>
            prev.map(seg => {
                if (seg.id !== analysis.segmentId) return seg;

                return {
                    ...seg,
                    entities: seg.entities.map(e => {
                        if (e.id !== entityId) return e;

                        return {
                            ...e,
                            facts: e.facts.map(f =>
                                f.id === factId ? { ...f, accepted, status: nextStatus } : f
                            )
                        };
                    })
                };
            })
        );
    };

    const updateFactDecisionByIdLocal = (factId, accepted) => {
        const nextStatus = accepted ? "approved" : "rejected";

        setAnalysis(prev => ({
            ...prev,
            entities: prev.entities.map(e => ({
                ...e,
                facts: e.facts.map(f =>
                    f.id === factId ? { ...f, accepted, status: nextStatus } : f
                )
            }))
        }));

        setSegments(prev =>
            prev.map(seg => {
                if (seg.id !== analysis.segmentId) return seg;

                return {
                    ...seg,
                    entities: seg.entities.map(e => ({
                        ...e,
                        facts: e.facts.map(f =>
                            f.id === factId ? { ...f, accepted, status: nextStatus } : f
                        )
                    }))
                };
            })
        );
    };

    const findFact = (factId) => {
        if (!analysis?.entities) return null;

        for (const entity of analysis.entities) {
            const found = (entity.facts || []).find(f => f.id === factId);
            if (found) {
                return { entity, fact: found };
            }
        }
        return null;
    };

    const getRelatedConflictFactIds = (factId) => {
        if (!analysis?.entities) return [factId];

        const selected = findFact(factId)?.fact;
        if (!selected) return [factId];

        const related = new Set([factId]);

        if (selected.conflictGroupId) {
            analysis.entities.forEach(entity => {
                (entity.facts || []).forEach(f => {
                    if (f.conflictGroupId && f.conflictGroupId === selected.conflictGroupId) {
                        related.add(f.id);
                    }
                });
            });
        }

        (selected.contradicts || []).forEach(id => related.add(id));

        analysis.entities.forEach(entity => {
            (entity.facts || []).forEach(f => {
                if ((f.contradicts || []).includes(factId)) {
                    related.add(f.id);
                }
            });
        });

        return Array.from(related);
    };

    const handleResolveConflict = async (entityId, factId) => {
        setSubmitError(null);

        const relatedFactIds = getRelatedConflictFactIds(factId);
        const acceptId = factId;
        const rejectIds = relatedFactIds.filter(id => id !== acceptId);

        try {
            await reviewFact(acceptId, "approved", "user", "Selected as canonical fact in conflict resolution");
            updateFactDecisionByIdLocal(acceptId, true);

            for (const rejectId of rejectIds) {
                await reviewFact(rejectId, "rejected", "user", `Rejected due to contradiction with canonical fact ${acceptId}`);
                updateFactDecisionByIdLocal(rejectId, false);
            }
        } catch (error) {
            console.error(error);
            setSubmitError(error.message || "Failed to resolve conflict");
        }
    };

    const getConflictFacts = () => {
        if (!analysis?.entities) return [];

        return analysis.entities.flatMap(entity =>
            (entity.facts || [])
                .filter(f => Boolean(f.conflictGroupId) || (Array.isArray(f.contradicts) && f.contradicts.length > 0))
                .map(f => ({
                    entityName: entity.name,
                    factId: f.id,
                    text: f.text,
                    conflictGroupId: f.conflictGroupId,
                    contradicts: f.contradicts || [],
                }))
        );
    };

    const handleAccept = async (entityId, factId) => {
        setSubmitError(null);

        const context = findFact(factId);
        const fact = context?.fact;
        const isConflictFact = Boolean(fact?.conflictGroupId) || (fact?.contradicts || []).length > 0;

        if (isConflictFact) {
            await handleResolveConflict(entityId, factId);
            return;
        }

        try {
            await reviewFact(factId, "approved", "user", null);
            updateFactDecisionLocal(entityId, factId, true);
        } catch (error) {
            console.error(error);
            setSubmitError(error.message || "Failed to save accepted fact");
        }
    };

    const handleReject = async (entityId, factId) => {
        setSubmitError(null);

        try {
            await reviewFact(factId, "rejected", "user", null);
            updateFactDecisionLocal(entityId, factId, false);
        } catch (error) {
            console.error(error);
            setSubmitError(error.message || "Failed to save rejected fact");
        }
    };

    const countPendingFacts = () => {
        if (!analysis || !analysis.entities) return 0;
        return analysis.entities.reduce((sum, entity) => {
            const pendingCount = entity.facts?.filter(f => {
                if (typeof f.status === "string") {
                    return f.status === "pending";
                }
                return f.accepted === undefined || f.accepted === null;
            }).length || 0;
            return sum + pendingCount;
        }, 0);
    };

    const handleSubmitReview = async () => {
        if (!analysis?.reviewSessionId) {
            setSubmitError("No review session found");
            return;
        }

        const pendingFacts = countPendingFacts();
        if (pendingFacts > 0) {
            setSubmitError("All facts must be addressed before submit");
            return;
        }

        setIsSubmittingReview(true);
        setSubmitError(null);
        setSubmitMessage(null);

        try {
            await submitReviewSession(analysis.reviewSessionId, "user");
            setSubmitMessage("Canon decisions submitted successfully!");
            setAnalysis(null);
        } catch(error) {
            console.error(error);
            setSubmitError(error.message || "Failed to submit review");
        } finally {
            setIsSubmittingReview(false);
        }
    };

    
  return (
    <>
    {/* loading overlay appears if segment is loading */}
    {loading && <LoadingOverlay {...loadingProgress} />}

    {error && <div className='inline-message error'>{error}</div>}
    {submitError && <div className='inline-message error'>{submitError}</div>}
    {submitMessage && <div className='inline-message success'>{submitMessage}</div>}

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
                <button type='submit' disabled={!segment.trim()}>
                    Submit
                </button>
            </div>
        </div>
    </form>

    {/* Only render below if analysis exists */}
        {analysis && (
            <div className='segment-summary'>
                <div>{analysis.summary}</div>
                <div className='segment-summary-meta'>Pending facts: {countPendingFacts()} | Conflicts: {analysis.conflictsDetected || 0}</div>
                {getConflictFacts().length > 0 && (
                    <div className='conflict-summary'>
                        <strong>Conflicting facts:</strong>
                        <ul>
                            {getConflictFacts().map((fact) => (
                                <li key={`${fact.factId}-${fact.entityName}`}>
                                    <span className='conflict-entity'>{fact.entityName}</span>: {fact.text}
                                    {fact.conflictGroupId && <span className='conflict-pill'>Group {fact.conflictGroupId}</span>}
                                    {fact.contradicts.length > 0 && <span className='conflict-links'>conflicts with {fact.contradicts.join(', ')}</span>}
                                </li>
                            ))}
                        </ul>
                    </div>
                )}
                <div className='segment-summary-actions'>
                    <button
                        type='button'
                        onClick={handleSubmitReview}
                        disabled={isSubmittingReview || countPendingFacts() > 0 || !analysis.reviewSessionId}
                    >
                        {isSubmittingReview ? "Submitting..." : "Submit Canon Decisions"}
                    </button>
                    {countPendingFacts() > 0 && <span className='segment-summary-hint'>Resolve all facts before submit.</span>}
                </div>
            </div>
        )}
        {analysis && analysis.entities.map(entity => (
            <EntityAnalysisCard
                key={entity.id}
                entity={entity}
                editable={true}
                onAccept={handleAccept}
                onReject={handleReject}
                onResolveConflict={handleResolveConflict}
            />
        ))}
    </>
  )
}

export default SegmentUpload