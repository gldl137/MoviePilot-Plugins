<script setup>
import Config from './Config.vue'

/**
 * 插件详情入口（宿主点击插件卡片时加载 ./Page 组件）。
 * 参考 CAS助手：点开插件直接进设置界面——这里直接复用 Config.vue 渲染配置表单；
 * Config 内部在 initialConfig 为空时会自动从 /config 接口拉取已保存配置。
 */
const props = defineProps({
  api: { type: Object, default: () => ({}) },
  pluginId: { type: String, default: '' },
  sourcePluginId: { type: String, default: '' },
  nativeSubscribe: { type: Function, default: null },
})
</script>

<template>
  <Config :api="props.api" :plugin-id="props.pluginId" />
</template>
