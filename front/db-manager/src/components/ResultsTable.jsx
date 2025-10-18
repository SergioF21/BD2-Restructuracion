import React from 'react';
import { Box, Typography } from '@mui/material';
import { DataGrid } from '@mui/x-data-grid';

export default function ResultsTable({ results, stats, currentPage, totalRows, onPageChange }) {
  const handlePageChange = (newPage) => {
    onPageChange(newPage + 1); // DataGrid usa 0-based, nosotros 1-based
  };

  return (
    <Box sx={{ flexGrow: 1, height: '400px' }}>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 1 }}>
        <Typography variant="h6" sx={{ marginBottom: '8px', color: 'text.primary' }}>
          Resultados de la consulta
        </Typography>
        <Typography variant="caption" color="textSecondary">{stats}</Typography>
      </Box>
      <DataGrid
        rows={results.rows || []}
        columns={results.columns || []}
        pagination
        page={currentPage - 1}
        pageSize={10}
        rowCount={totalRows}
        paginationMode="server"
        onPageChange={handlePageChange}
        loading={!results.rows}
        density="compact"
        autoHeight={false}
      />
    </Box>
  );
}