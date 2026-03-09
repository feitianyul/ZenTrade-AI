import { defineConfig, loadEnv } from 'vite'

// 纯静态 SPA，无 Vue/TypeScript 依赖
export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), '')
  return {
    server: {
      host: env.VITE_DEV_HOST || '127.0.0.1',
      port: Number(env.VITE_DEV_PORT || 5174),
    },
    plugins: [
      {
        name: 'admin-feitian-rewrite',
        configureServer(server) {
          server.middlewares.use((req, res, next) => {
            const url = req.url?.split('?')[0] ?? ''
            if (url === '/admin/feitian' || url === '/admin/feitian/') {
              req.url = '/admin/feitian/index.html'
            }
            next()
          })
        },
      },
    ],
  }
})
