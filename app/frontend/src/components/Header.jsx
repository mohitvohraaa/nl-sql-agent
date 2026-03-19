import React from 'react';
import './Header.css';

export default function Header() {
  return (
    <header className="header">
      <div className="logo">
        <span className="dot" />
        <span className="title">NL → SQL AGENT</span>
      </div>
      <span className="subtitle">GA4 · BigQuery · Groq · LangGraph</span>
    </header>
  );
}