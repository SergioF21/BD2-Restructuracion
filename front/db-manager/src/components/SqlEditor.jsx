import React from 'react';
import Editor from '@monaco-editor/react';

export default function SqlEditorComponent({ value, onChange }) {
  return (
    <Editor
      height="100%"
      language="sql"
      theme="vs-light" // Puedes cambiar a "vs-dark"
      value={value}
      onChange={onChange}
      options={{
        minimap: { enabled: false },
        fontSize: 14,
        wordWrap: 'on',
      }}
    />
  );
}