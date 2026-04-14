import StatCard from "./StatCard"

const InfoCards = ({project}) => {
  const stats = project?.stats || {};


  return (
    <div className="info-cards-grid">
      <StatCard label = "Total Facts Extracted" value={stats.totalFactsExtracted ?? 0}/>
      <StatCard label="Total Segments" value={stats.totalSegments ?? 0}/>
      <StatCard label="Total Facts" value={stats.totalFacts ?? 0}/>
      <StatCard label="Pending Reviews" value={stats.pendingReviews ?? 0}/>
      <StatCard label="Accepted Facts" value={stats.acceptedFacts ?? 0}/>
      <StatCard label="Rejected Facts" value={stats.rejectedFacts ?? 0}/>
    </div>
  )
}

export default InfoCards