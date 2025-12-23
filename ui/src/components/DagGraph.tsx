import { useCallback, useEffect, useMemo, useState } from 'react'
import {
  ReactFlow,
  Background,
  Controls,
  MiniMap,
  useNodesState,
  useEdgesState,
  MarkerType,
  Panel,
  type Node,
  type Edge,
} from '@xyflow/react'
import dagre from 'dagre'
import '@xyflow/react/dist/style.css'

interface DagNode {
  md5: string | null
  size: number | null
  cmd: string | null
  deps: Record<string, string>
  is_dir: boolean
}

interface DagData {
  nodes: Record<string, DagNode>
  edges: { from: string; to: string }[]
}

interface NodeDetailsProps {
  node: { id: string; data: DagNode } | null
  onClose: () => void
}

function NodeDetails({ node, onClose }: NodeDetailsProps) {
  if (!node) return null

  const { id, data } = node
  const deps = Object.keys(data.deps)

  return (
    <div className="node-details">
      <div className="node-details-header">
        <h3>{id}{data.is_dir ? '/' : ''}</h3>
        <button onClick={onClose}>&times;</button>
      </div>
      {data.md5 && <p><strong>MD5:</strong> {data.md5.slice(0, 8)}...</p>}
      {data.size && <p><strong>Size:</strong> {data.size.toLocaleString()} bytes</p>}
      {data.cmd && (
        <div>
          <strong>Command:</strong>
          <pre className="cmd">{data.cmd}</pre>
        </div>
      )}
      {deps.length > 0 && (
        <div>
          <strong>Dependencies ({deps.length}):</strong>
          <ul className="deps-list">
            {deps.map(dep => (
              <li key={dep}>{dep}</li>
            ))}
          </ul>
        </div>
      )}
    </div>
  )
}

// Use dagre for automatic DAG layout
function getLayoutedElements(
  nodes: Node[],
  edges: Edge[],
  direction = 'TB'
): { nodes: Node[]; edges: Edge[] } {
  const dagreGraph = new dagre.graphlib.Graph()
  dagreGraph.setDefaultEdgeLabel(() => ({}))

  const nodeWidth = 180
  const nodeHeight = 40

  dagreGraph.setGraph({
    rankdir: direction,
    nodesep: 50,
    ranksep: 80,
    marginx: 20,
    marginy: 20,
  })

  nodes.forEach((node) => {
    dagreGraph.setNode(node.id, { width: nodeWidth, height: nodeHeight })
  })

  edges.forEach((edge) => {
    dagreGraph.setEdge(edge.source, edge.target)
  })

  dagre.layout(dagreGraph)

  const layoutedNodes = nodes.map((node) => {
    const nodeWithPosition = dagreGraph.node(node.id)
    return {
      ...node,
      position: {
        x: nodeWithPosition.x - nodeWidth / 2,
        y: nodeWithPosition.y - nodeHeight / 2,
      },
    }
  })

  return { nodes: layoutedNodes, edges }
}

function getNodeType(id: string, data: DagNode, edges: Edge[]): 'root' | 'leaf' | 'middle' {
  const hasDeps = Object.keys(data.deps).length > 0
  const hasDependents = edges.some(e => e.source === id)

  if (!hasDeps) return 'root'
  if (!hasDependents) return 'leaf'
  return 'middle'
}

function getNodeStyle(type: 'root' | 'leaf' | 'middle', isDir: boolean) {
  const colors = {
    root: '#4CAF50',   // green
    leaf: '#2196F3',   // blue
    middle: '#FF9800', // orange
  }

  return {
    background: colors[type],
    color: 'white',
    border: isDir ? '2px dashed #333' : '1px solid #333',
    borderRadius: 4,
    padding: '8px 12px',
    fontSize: 11,
    fontFamily: 'monospace',
  }
}

interface Props {
  data: DagData | null
  filter: string
}

export function DagGraph({ data, filter }: Props) {
  const [nodes, setNodes, onNodesChange] = useNodesState([])
  const [edges, setEdges, onEdgesChange] = useEdgesState([])
  const [selectedNode, setSelectedNode] = useState<{ id: string; data: DagNode } | null>(null)

  // Convert data to React Flow format
  const { flowNodes, flowEdges } = useMemo(() => {
    if (!data) return { flowNodes: [], flowEdges: [] }

    // Filter nodes
    const filteredNodeIds = new Set(
      Object.keys(data.nodes).filter(id =>
        !filter || id.toLowerCase().includes(filter.toLowerCase())
      )
    )

    // Build edges first to determine node types
    const flowEdges: Edge[] = data.edges
      .filter(e => filteredNodeIds.has(e.from) || filteredNodeIds.has(e.to))
      .map(({ from, to }) => ({
        id: `${from}-${to}`,
        source: from,
        target: to,
        markerEnd: { type: MarkerType.ArrowClosed },
        style: { stroke: '#999' },
      }))

    // Also include nodes that are connected via edges
    flowEdges.forEach(e => {
      if (data.nodes[e.source]) filteredNodeIds.add(e.source)
      if (data.nodes[e.target]) filteredNodeIds.add(e.target)
    })

    const flowNodes: Node[] = Object.entries(data.nodes)
      .filter(([id]) => filteredNodeIds.has(id))
      .map(([id, nodeData]) => {
        const type = getNodeType(id, nodeData, flowEdges)
        const label = id.split('/').slice(-2).join('/')  // Show last 2 path segments

        return {
          id,
          data: { label, ...nodeData },
          position: { x: 0, y: 0 },
          style: getNodeStyle(type, nodeData.is_dir),
        }
      })

    return { flowNodes, flowEdges }
  }, [data, filter])

  // Apply layout
  useEffect(() => {
    if (flowNodes.length > 0) {
      const { nodes: layoutedNodes, edges: layoutedEdges } = getLayoutedElements(
        flowNodes,
        flowEdges
      )
      setNodes(layoutedNodes)
      setEdges(layoutedEdges)
    }
  }, [flowNodes, flowEdges, setNodes, setEdges])

  const onNodeClick = useCallback((_: React.MouseEvent, node: Node) => {
    const nodeData = data?.nodes[node.id]
    if (nodeData) {
      setSelectedNode({ id: node.id, data: nodeData })
    }
  }, [data])

  if (!data) {
    return (
      <div className="dag-loading">
        <p>Loading DAG data...</p>
        <p className="hint">Generate with: <code>dvx dag --json &gt; dag.json</code></p>
      </div>
    )
  }

  return (
    <div className="dag-container">
      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onNodeClick={onNodeClick}
        fitView
        attributionPosition="bottom-left"
      >
        <Background />
        <Controls />
        <MiniMap
          nodeColor={(n) => n.style?.background as string || '#ccc'}
          maskColor="rgba(0,0,0,0.1)"
        />
        <Panel position="top-left" className="legend">
          <div className="legend-item">
            <span className="legend-dot" style={{ background: '#4CAF50' }} />
            Root (no deps)
          </div>
          <div className="legend-item">
            <span className="legend-dot" style={{ background: '#2196F3' }} />
            Leaf (no dependents)
          </div>
          <div className="legend-item">
            <span className="legend-dot" style={{ background: '#FF9800' }} />
            Intermediate
          </div>
        </Panel>
      </ReactFlow>
      {selectedNode && (
        <NodeDetails
          node={selectedNode}
          onClose={() => setSelectedNode(null)}
        />
      )}
    </div>
  )
}
