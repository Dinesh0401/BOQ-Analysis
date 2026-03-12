import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      '/extract': 'http://localhost:8000',
      '/upload-excel': 'http://localhost:8000',
      '/analyze': 'http://localhost:8000',
      '/risk': 'http://localhost:8000',
      '/graph-stats': 'http://localhost:8000',
    },
  },
})
