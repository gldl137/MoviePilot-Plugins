<script setup>
const props = defineProps({
  api: { type: Object, default: () => ({}) },
  pluginId: { type: String, default: '' },
  sourcePluginId: { type: String, default: '' },
  nativeSubscribe: { type: Function, default: null },
})
</script>

<template>
  <div class="pa-4">
    CAS 恢复源文件 · 恢复记录请查看左侧菜单「媒体整理 → CAS恢复记录」。
  </div>
</template>
