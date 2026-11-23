import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'
import { federation } from '@module-federation/vite'

// MoviePilot Vue 联邦插件：vue / vuetify 由主应用注入（import:false），
// 插件只暴露远程组件，入口产物为 assets/remoteEntry.js。
export default defineConfig({
  plugins: [
    vue(),
    federation({
      name: 'CasRestore',
      filename: 'assets/remoteEntry.js',
      exposes: {
        './AppPage': './src/components/AppPage.vue',
        './Config': './src/components/Config.vue',
        './Page': './src/components/Page.vue',
      },
      dts: false,
      shared: {
        vue: { requiredVersion: false, import: false },
        vuetify: { requiredVersion: false, import: false, singleton: true },
        'vuetify/styles': { requiredVersion: false, import: false, singleton: true },
      },
    }),
  ],
  build: {
    target: 'esnext',
    minify: false,
    cssCodeSplit: true,
  },
})
