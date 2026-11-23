// 仅用于本地 dev 预览；远程组件由模块联邦暴露，主应用加载 AppPage/Config/Page。
import { createApp } from 'vue'
import App from './App.vue'

createApp(App).mount('#app')
