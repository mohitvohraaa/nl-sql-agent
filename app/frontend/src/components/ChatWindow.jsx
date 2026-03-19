import React from 'react';
import UserMessage from './UserMessage';
import AgentMessage from './AgentMessage';
import EmptyState from './EmptyState';
import Loading from './Loading';
import './ChatWindow.css';

export default function ChatWindow({ messages, loading, suggestions, onSuggestion, bottomRef }) {
  return (
    <div className="chat-window">
      {messages.length === 0 && !loading
        ? <EmptyState suggestions={suggestions} onSuggestion={onSuggestion} />
        : (
          <>
            {messages.map((msg, i) =>
              msg.role === 'user'
                ? <UserMessage key={i} text={msg.text} />
                : <AgentMessage key={i} data={msg.data} />
            )}
            {loading && <Loading />}
          </>
        )
      }
      <div ref={bottomRef} />
    </div>
  );
}