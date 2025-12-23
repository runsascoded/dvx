import { useState, useEffect, useCallback } from 'react'
import { DagGraph } from './components/DagGraph'
import './App.css'

interface DagData {
  nodes: Record<string, unknown>
  edges: { from: string; to: string }[]
}

function App() {
  const [data, setData] = useState<DagData | null>(null)
  const [filter, setFilter] = useState('')
  const [error, setError] = useState<string | null>(null)

  // Try to load dag.json from current directory or public folder
  useEffect(() => {
    const loadData = async () => {
      // Try multiple locations
      const urls = [
        '/dag.json',
        './dag.json',
        'dag.json',
      ]

      for (const url of urls) {
        try {
          const res = await fetch(url)
          if (res.ok) {
            const json = await res.json()
            setData(json)
            setError(null)
            return
          }
        } catch {
          // Try next URL
        }
      }

      setError('Could not load dag.json. Generate it with: dvx dag --json > ui/public/dag.json')
    }

    loadData()
  }, [])

  const handleFileUpload = useCallback((e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0]
    if (!file) return

    const reader = new FileReader()
    reader.onload = (event) => {
      try {
        const json = JSON.parse(event.target?.result as string)
        setData(json)
        setError(null)
      } catch {
        setError('Invalid JSON file')
      }
    }
    reader.readAsText(file)
  }, [])

  return (
    <div className="app">
      <header className="header">
        <h1>DVX DAG Viewer</h1>
        <div className="controls">
          <input
            type="text"
            placeholder="Filter nodes..."
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
            className="filter-input"
          />
          <label className="upload-btn">
            Upload dag.json
            <input
              type="file"
              accept=".json"
              onChange={handleFileUpload}
              style={{ display: 'none' }}
            />
          </label>
        </div>
      </header>

      {error && (
        <div className="error">
          {error}
        </div>
      )}

      <main className="main">
        <DagGraph data={data} filter={filter} />
      </main>

      <footer className="footer">
        {data && (
          <span>
            {Object.keys(data.nodes).length} nodes, {data.edges.length} edges
          </span>
        )}
      </footer>
    </div>
  )
}

export default App
