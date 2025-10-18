import React, { useState } from 'react';
import { Box, Button, TextField, InputAdornment } from '@mui/material';
import SearchIcon from '@mui/icons-material/Search';

export default function Header({ onSearchTables }) {
  const [searchTerm, setSearchTerm] = useState('');

  const handleSearchChange = (event) => {
    const value = event.target.value;
    setSearchTerm(value);
    onSearchTables(value);
  };

  return (
    <Box sx={{
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'space-between',
      padding: '8px 16px',
      borderBottom: '1px solid #ddd',
      backgroundColor: 'white'
    }}>
      <TextField
        size="small"
        variant="outlined"
        placeholder="Buscar Tabla..."
        value={searchTerm}
        onChange={handleSearchChange}
        InputProps={{
          startAdornment: (
            <InputAdornment position="start">
              <SearchIcon />
            </InputAdornment>
          ),
        }}
      />
      <Box>
        <Button variant="contained" sx={{ mr: 1 }}>Nueva Consulta</Button>
        <Button variant="outlined">Exportar</Button>
      </Box>
    </Box>
  );
}