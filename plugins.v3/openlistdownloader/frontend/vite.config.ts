import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'
import federation from '@originjs/vite-plugin-federation'

// 插件名必须与后端插件类名一致（OpenListDownloader），MP 前端据此定位远程组件
const PLUGIN_ID = 'OpenListDownloader'

export default defineConfig({
  plugins: [
    vue(),
    federation({
      name: PLUGIN_ID,
      filename: 'remoteEntry.js',
      exposes: {
        './AppPage': './src/components/AppPage.vue',
        './Page': './src/components/Page.vue',
        './Config': './src/components/Config.vue',
      },
      shared: {
        vue: {
          requiredVersion: false,
          generate: false,
        },
        vuetify: {
          requiredVersion: false,
          generate: false,
        },
      },
    }),
  ],
  build: {
    outDir: '../dist',
    emptyOutDir: false,
    target: 'esnext',
    minify: false,
    cssCodeSplit: true,
  },
})
