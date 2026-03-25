import { useState, useEffect } from 'react'
import { extractEntities } from '../../services/aiService';
import EntityAnalysisCard from '../../components/ProjectLayout/EntityAnalysisCard';
import LoadingOverlay from '../../components/ProjectLayout/LoadingOverlay';
import projectService from '../../services/projectService';

const SegmentUpload = ({setSegments}) => {
    const [segment,setSegment] = useState("");
    const [analysis, setAnalysis] = useState(null);
    const [loading, setLoading] = useState(false);
    const [segmentTitle, setSegmentTitle] = useState("");
    const [progress, setProgress] = useState(0);
    const [step, setStep] = useState("1/3");
    const [subText, setSubText] = useState("");
    const [eta, setEta] = useState("calculating…");

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

        setError(null);
        setSubmitError(null);
        setSubmitMessage(null);
        setLoading(true); // show the loading overlayyy

        try {
            //load backend
            const result = await extractEntities(segment);

            const newSegment = {
                id: Date.now(),
                title: segmentTitle.trim() || `Segment ${Date.now()}`, // inputted title or fallback title
                summary:result.summary,
                text:segment,
                entities:result.entities
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

    const handleAccept = (entityId, factId) => {
        setAnalysis(prev => ({
            ...prev,
            entities: prev.entities.map(e => {
                if (e.id !== entityId) return e;

                return {
                    ...e,
                    facts: e.facts.map(f =>
                        f.id === factId ? { ...f, accepted: true } : f
                    )
                };
            })
        }));

        // update the correct segment
        setSegments(prev =>
            prev.map(seg => {
                if (seg.id !== analysis.segmentId) return seg;

                return {
                    ...seg,
                    entities: seg.entities.map(e => {
                        if (e.id !== entityId) return e;

                        return{
                            ...e,
                            facts: e.facts.map(f => 
                                f.id === factId ? {...f,accepted:true} : f
                            )
                        }
                    })
                }
            })
        )
    };

    const handleReject = (entityId, factId) => {
        setAnalysis(prev => ({
            ...prev,
            entities: prev.entities.map(e => {
                if (e.id !== entityId) return e;

                return {
                    ...e,
                    facts: e.facts.map(f =>
                        f.id === factId ? { ...f, accepted: false } : f
                    )
                };
            })
        }));

        // update the correct segment
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
                                f.id === factId ? { ...f, accepted: false } : f
                            )
                        };
                    })
                };
            })
        );
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
                <button type='submit' disabled={!segment.trim()}>
                    Submit
                </button>
            </div>
        </div>
    </form>

    {/* Only render below if analysis exists */}
        {analysis && (
            <div className='segment-summary' style={{padding:"2rem", backgroundColor:"red",margin:"5px", borderRadius:"8px" }}>
                <div>{analysis.summary}</div>
                <div style={{marginTop: "0.5rem"}}>Pending facts: {analysis.pendingFactsCount || 0} | Conflicts: {analysis.conflictsDetected || 0}</div>
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
                editable={true}
                onAccept={handleAccept}
                onReject={handleReject}
            />
        ))}
    </>
  )
}

export default SegmentUpload