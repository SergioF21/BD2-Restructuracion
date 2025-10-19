import React from 'react';
import { Box, Typography } from '@mui/material';
import { DataGrid } from '@mui/x-data-grid';

export default function ResultsTable({ results, stats, currentPage, totalRows, onPageChange, loading = false }) {
  const handlePageChange = (newPage) => {
    onPageChange(newPage + 1); // DataGrid usa 0-based, nosotros 1-based
  };
  // Normalize results shape: { data: { columns, rows }, totalRows } or legacy { columns, rows }
  let cols = [];
  let rows = [];
  if (results) {
    if (results.data) {
      cols = results.data.columns || [];
      rows = results.data.rows || [];
    } else {
      cols = results.columns || [];
      rows = results.rows || [];
    }
  }

  // Convert columns (array of strings) to DataGrid column defs
  const dgColumns = (Array.isArray(cols) ? cols : []).map(c => ({ field: c, headerName: String(c), flex: 1 }));

  // Ensure each row has an 'id' field (DataGrid requirement)
  const dgRows = (Array.isArray(rows) ? rows : []).map((r, i) => ({ id: r.id ?? i, ...r }));

  return (
    <Box sx={{ flexGrow: 1, height: '400px' }}>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 1 }}>
        <Typography variant="h6" sx={{ marginBottom: '8px', color: 'text.primary' }}>
          Resultados de la consulta
        </Typography>
  <Typography variant="caption" sx={{ color: 'text.secondary' }}>{stats}</Typography>
      </Box>
      <DataGrid
        rows={dgRows}
        columns={dgColumns}
        pagination
        page={currentPage - 1}
        pageSize={10}
        rowCount={totalRows}
        paginationMode="server"
        onPageChange={handlePageChange}
        loading={!!loading}
        density="compact"
        autoHeight={false}
      />
    </Box>
  );
}