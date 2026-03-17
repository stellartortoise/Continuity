
const Hero = ({project}) => {

  return (
    <div className="hero-container">
      <div className="hero-image">
        <img src="../../resources/starSplash4.jpg" className="img-fluid" alt={project.name}/>
      </div>
      <div className="hero-overlay">
        <h2>Welcome back to:</h2>
        <h1>{project.name}</h1>
      </div>
    </div>
  )
}

export default Hero