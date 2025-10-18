import React from 'react';
import { Box, Typography, List, ListItemButton, ListItemIcon, ListItemText } from '@mui/material';
import TableRowsIcon from '@mui/icons-material/TableRows';

export default function Sidebar({ tables = [] }) {
  return (
    <Box sx={{ 
      width: '100%', 
      height: '100%', 
      backgroundColor: '#2d3748',
      color: 'white',
      padding: '8px' 
    }}>
      <Typography variant="h6" sx={{ padding: '16px 16px 8px' }}>
        Tablas ({tables.length})
      </Typography>
      <List dense>
        {tables.map(tabla => (
          <ListItemButton key={tabla}>
            <ListItemIcon>
              <TableRowsIcon sx={{ color: '#9f7aea' }} />
            </ListItemIcon>
            <ListItemText primary={tabla} />
          </ListItemButton>
        ))}
      </List>
      {tables.length === 0 && (
        <Typography variant="caption" sx={{ padding: '16px', color: '#a0aec0' }}>
          No se encontraron tablas
        </Typography>
      )}
    </Box>
  );
}