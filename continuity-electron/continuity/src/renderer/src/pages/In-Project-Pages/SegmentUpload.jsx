import { useState } from 'react'
import { extractEntities } from '../../services/aiService';
import EntityAnalysisCard from '../../components/ProjectLayout/EntityAnalysisCard';
import LoadingOverlay from '../../components/ProjectLayout/LoadingOverlay';

const SegmentUpload = ({setSegments}) => {
    const [segment,setSegment] = useState("");
    const [analysis, setAnalysis] = useState(null);
    const [loading, setLoading] = useState(false);

    const handleSubmit = async(e) => {
        e.preventDefault();

        if (!segment.trim()) return;

        setLoading(true); // show the loading overlayyy
        try {
            //load backend
            const result = await extractEntities(segment);
            setAnalysis(result);

            // Add new segment to app level state
            setSegments(prev => [...prev, {
                id: Date.now(),
                title: `Segment ${prev.length + 1}`,
                summary: result.summary,
                text: segment,
                entities: result.entities
            }])
        } catch(error){
            console.error(error);
        } finally {
            setLoading(false); // hide the overlay
        }


    }

    const handleAccept = (entityId, factId) => {
        setAnalysis(prev => ({
            ...prev,
            entities: prev.entities.map(e => {
                if (e.id !== entityId) return e; // leave other entities unchanged

                return {
                    ...e,
                    facts: e.facts.map(f =>
                        f.id === factId ? { ...f, accepted: true } : f
                    )
                };
            })
        }));
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
    };

    
  return (
    <>
    {/* loading overlay appears if segment is loading */}
    {loading && <LoadingOverlay/>}

    <form className='segmentUpload' onSubmit={handleSubmit}>
        <label htmlFor='userSegment'>Submit your Story Segment:</label>

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
            <div className='segment-summary' style={{padding:"2rem", backgroundColor:"red",margin:"5px", borderRadius:"8px"} }>{analysis.summary}</div>
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