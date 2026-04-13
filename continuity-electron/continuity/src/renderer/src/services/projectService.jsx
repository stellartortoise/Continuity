const STORAGE_KEY = "currentProjectId";
const API_BASE = "http://localhost:8001";

const projectService = {
  async createProject(name, description = "") {
    const params = new URLSearchParams({ name, description });
    const res = await fetch(`${API_BASE}/projects?${params.toString()}`, {
      method: "POST",
    });
    if (!res.ok) {
      throw new Error(`Failed to create project (${res.status})`);
    }

    const project = await res.json();
    localStorage.setItem(STORAGE_KEY, project.id);
    return project;
  },

  async loadProject() {
    const projectId = localStorage.getItem(STORAGE_KEY);
    if (!projectId) {
      return null;
    }

    const params = new URLSearchParams({ id: projectId });
    const res = await fetch(`${API_BASE}/projects?${params.toString()}`);
    if (!res.ok) {
      return null;
    }
    return res.json();
  },

  async saveProject(project) {
    const params = new URLSearchParams({
      name: project.name || "Untitled",
      description: project.description || "",
    });
    await fetch(`${API_BASE}/projects/${project.id}?${params.toString()}`, { method: "PUT" });
    localStorage.setItem(STORAGE_KEY, project.id);
  },

  async deleteProject(projectId) {
    if (!projectId) return;
    const res = await fetch(`${API_BASE}/projects/${projectId}`, { method: "DELETE" });
    if (!res.ok) {
      throw new Error(`Failed to delete project (${res.status})`);
    }
  },

  async clearProject() {
    localStorage.removeItem(STORAGE_KEY);
  }
}

export default projectService