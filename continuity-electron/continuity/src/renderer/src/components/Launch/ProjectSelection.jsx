import { useEffect, useRef, useState } from "react";
import {useNavigate} from "react-router-dom"
import projectService from "../../services/projectService";

const ProjectSelection = () => {
    const navigate = useNavigate();
    const [project, setProject] = useState(null);
    const [projectName, setProjectName] = useState("")
    const [isCreating, setIsCreating] = useState(false)
    const [error, setError] = useState(null)
    const [busy, setBusy] = useState(false)
    const inputRef = useRef(null)

    useEffect(()=> {
        const checkProject = async ()=> {
            const existing = await projectService.loadProject();
            setProject(existing);
        }
        checkProject();
    }, [])

    useEffect(() => {
        if (!project && isCreating) {
            inputRef.current?.focus();
        }
    }, [project, isCreating])

    const handleCreateStart = () => {
        setError(null);
        setIsCreating(true);
    }

    const handleCreateConfirm = async ()=> {
        if(!projectName.trim()) return;

        setBusy(true);
        setError(null);
        try {
            await projectService.createProject(projectName.trim());
            navigate("/project");
        } catch (e) {
            setError(e.message || "Failed to create project");
        } finally {
            setBusy(false);
        }
    }

    const handleLoad = () => {
        navigate("/project");
    }

    const handleRemoveProject = async () =>{
        const RUSure = window.confirm("Are you sure you want to remove this project? This action cannot be undone.");
        if(!RUSure)return;

        setBusy(true);
        setError(null);
        try {
            if (project?.id) {
                await projectService.deleteProject(project.id);
            }
            await projectService.clearProject();
            setProject(null);
            setProjectName("");
            setIsCreating(true);
        } catch (e) {
            setError(e.message || "Failed to delete project");
        } finally {
            setBusy(false);
        }
    }

  return (
    <>
    <div className='card'>
        <div className='card-body'>
            <div className='card-text'>
                {project ? (
                    <>
                    <h3>Load Existing Project</h3>
                    <p>{project.name}</p>
                    </>
                ):(
                    <>
                    <h3>Create a new Project</h3>
                    <p>Create a new Continuity project with a new body of work.</p>
                    </>
                    
                )}
            </div>

            <div className='card-button'>
                {project ? (
                    <button className="LoadBtn" onClick={handleLoad} disabled={busy}>Load Project</button>
                ):(
                    !isCreating ? (
                        <button className="CreateBtn" onClick={handleCreateStart} disabled={busy}>Create New Project</button>
                    ):(
                        <>
                        <input ref={inputRef} className="progNameInput" type="text" value={projectName} onChange={(e)=>setProjectName(e.target.value)} placeholder="Enter Project Name" disabled={busy}/>
                        <button className="CreateBtn" onClick={handleCreateConfirm} disabled={busy || !projectName.trim()}>Confirm Project Name</button>
                        </>
                    )
                )}
                {project && (
                    <button className="DeleteBtn" onClick={handleRemoveProject} disabled={busy}>Delete Project</button>
                )}
                {error && <p style={{color: "#f28", margin: 0}}>{error}</p>}
            </div>
        </div>
    </div>
    </>
  )
}

export default ProjectSelection