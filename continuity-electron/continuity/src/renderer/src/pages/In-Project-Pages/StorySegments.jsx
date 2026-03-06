import { useState, useEffect } from "react"
import SegmentTextBlock from "../../components/ProjectLayout/SegmentTextBlock";
import EntityAnalysisCard from "../../components/ProjectLayout/EntityAnalysisCard";


const StorySegments = ({uploadedSegments}) => {
  const [openId, setOpenId] = useState(null);


  const toggleCard = (id) => {
    setOpenId(prev => (prev=== id ? null : id));
  }

  return (
    <div className='story-segments'>
      <h1>My Story Segments</h1>
      
      {uploadedSegments.map(segment => (
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

              {/* Entity cards - read only */}
              <div style={{ flexShrink: 0, minWidth: "250px" }}>
              {segment.entities.map(entity =>(
                <EntityAnalysisCard 
                  key={entity.id} 
                  entity={entity} 
                  editable={false}
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