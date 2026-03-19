import React, { useState, useRef, useEffect } from 'react'
import Header from './components/Header'
import ChatWindow from './components/ChatWindow'
import InputBar from './components/InputBar'
import './App.css'

const SUGGESTIONS = [
  "What are the top 5 products by revenue?",
  "How many unique users visited each day?",
  "What are the most common event types?",
  "What is the total revenue by month?",
]

export default function App() {
  const [messages, setMessages] = useState([])
  const [loading, setLoading]   = useState(false)
  const bottomRef = useRef(null)

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages, loading])

  const sendMessage = async (text) => {
    if (!text.trim() || loading) return

    setMessages(prev => [...prev, { role: 'user', text }])
    setLoading(true)

    try {
      const res = await fetch('/chat', {
        method:  'POST',
        headers: { 'Content-Type': 'application/json' },
        body:    JSON.stringify({ message: text }),
      })
      const data = await res.json()
      setMessages(prev => [...prev, { role: 'agent', data }])
    } catch (err) {
      setMessages(prev => [...prev, {
        role: 'agent',
        data: { error: 'Could not reach the backend. Is it running on port 8000?' }
      }])
    }

    setLoading(false)
  }

  return (
    <div className="app">
      <Header />
      <ChatWindow
        messages={messages}
        loading={loading}
        suggestions={SUGGESTIONS}
        onSuggestion={sendMessage}
        bottomRef={bottomRef}
      />
      <InputBar onSend={sendMessage} loading={loading} />
    </div>
  )
}