<script setup lang="ts">
import { ref, onMounted } from 'vue'

const props = defineProps({
  navKey: { type: String, default: 'main' },
  initialConfig: { type: Object, default: () => ({}) },
  api: { type: Object, default: () => ({}) },
  pluginId: { type: String, default: '' },
})

// switch 事件：通知主前端从详情页切到设置弹窗（./Config）
const emit = defineEmits(['switch', 'action', 'close'])

const PLUGIN_ID = 'OpenListDownloader'

const loading = ref(false)
const clearing = ref(false)
const error = ref('')
const notice = ref('')
const noticeVisible = ref(false)
let noticeTimer: any = null
const stats = ref({
  total: 0,
  last_time: '',
  total_size: 0,
})

function humanSize(bytes?: number): string {
  if (!bytes || bytes <= 0) return '0 B'
  const units = ['B', 'KB', 'MB', 'GB', 'TB']
  let size = Number(bytes)
  let i = 0
  while (size >= 1024 && i < units.length - 1) {
    size /= 1024
    i++
  }
  return `${size.toFixed(i === 0 ? 0 : 2)} ${units[i]}`
}

async function loadStats() {
  loading.value = true
  error.value = ''
  try {
    let data: any = null
    if (props.api && typeof props.api.get === 'function') {
      const res = await props.api.get(`plugin/${PLUGIN_ID}/history`)
      data = res
    } else {
      const res = await fetch(`/api/v1/plugin/${PLUGIN_ID}/history`)
      data = await res.json()
    }
    const list = (data?.items || data?.data || (Array.isArray(data) ? data : [])) as any[]
    const total = Number(data?.count ?? list?.length ?? 0)
    const last = list && list.length ? list[0] : null
    const totalSize = (list || []).reduce((acc: number, it: any) => acc + (Number(it.size) || 0), 0)
    stats.value = {
      total,
      last_time: last?.time || '',
      total_size: totalSize,
    }
  } catch (e: any) {
    error.value = String(e?.message || e)
  } finally {
    loading.value = false
  }
}

function showNotice(msg: string) {
  notice.value = msg
  noticeVisible.value = true
  if (noticeTimer) clearTimeout(noticeTimer)
  noticeTimer = setTimeout(() => { noticeVisible.value = false }, 2500)
}

async function clearRecords() {
  clearing.value = true
  try {
    let res: any = null
    if (props.api && typeof props.api.post === 'function') {
      res = await props.api.post(`plugin/${PLUGIN_ID}/clear_records`, {})
    } else {
      const r = await fetch(`/api/v1/plugin/${PLUGIN_ID}/clear_records`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({}),
      })
      res = await r.json()
    }
    if (res?.success === false) {
      showNotice(res?.message || '清除失败')
    } else {
      showNotice(res?.message || '已清除')
      await loadStats()
    }
  } catch (e: any) {
    showNotice(String(e?.message || e))
  } finally {
    clearing.value = false
  }
}

onMounted(loadStats)
</script>

<template>
  <div class="dash-page">
    <!-- 右上角关闭按钮 -->
    <v-btn
      icon="mdi-close"
      variant="text"
      size="small"
      class="dash-close"
      @click="emit('close')"
    />

    <v-progress-linear v-if="loading" indeterminate color="primary" />

    <v-alert v-if="error" type="error" variant="tonal" class="my-3">{{ error }}</v-alert>

    <!-- 统计卡片区（上面） -->
    <div class="dash-content">
      <v-row class="dash-sub">
        <v-col cols="12" md="4">
          <v-card variant="tonal" flat class="dash-sub-card">
            <div class="dash-sub-label">总下载文件数</div>
            <div class="dash-sub-num">{{ stats.total }}</div>
          </v-card>
        </v-col>
        <v-col cols="12" md="4">
          <v-card variant="tonal" flat class="dash-sub-card">
            <div class="dash-sub-label">累计下载大小</div>
            <div class="dash-sub-num">{{ humanSize(stats.total_size) }}</div>
          </v-card>
        </v-col>
        <v-col cols="12" md="4">
          <v-card variant="tonal" flat class="dash-sub-card">
            <div class="dash-sub-label">最近下载时间</div>
            <div class="dash-sub-num" style="font-size: 16px">{{ stats.last_time || '-' }}</div>
          </v-card>
        </v-col>
      </v-row>
    </div>

    <!-- 右下角：设置 + 清除 -->
    <div class="dash-actions">
      <v-btn
        color="primary"
        variant="tonal"
        prepend-icon="mdi-cog-outline"
        class="mr-2"
        @click="emit('switch')"
      >
        设置
      </v-btn>
      <v-btn
        color="error"
        variant="tonal"
        prepend-icon="mdi-delete-sweep-outline"
        :loading="clearing"
        @click="clearRecords"
      >
        清除
      </v-btn>
    </div>

    <!-- 操作提示 -->
    <v-snackbar
      v-model="noticeVisible"
      color="success"
      location="bottom"
      timeout="2500"
    >
      {{ notice }}
    </v-snackbar>
  </div>
</template>

<style scoped>
.dash-page {
  padding: 24px;
  position: relative;
  min-height: 260px;
  display: flex;
  flex-direction: column;
}
.dash-close {
  position: absolute;
  top: 8px;
  right: 8px;
  z-index: 2;
}
.dash-content {
  flex: 1;
}
.dash-hero {
  margin-top: 8px;
}
.dash-actions {
  display: flex;
  justify-content: flex-end;
  align-items: center;
  margin-top: 16px;
}
.dash-hero-card {
  padding: 24px 16px;
  text-align: center;
}
.dash-hero-icon {
  margin: 0 auto;
}
.dash-hero-num {
  font-size: 48px;
  font-weight: 700;
  line-height: 1.1;
  color: rgb(var(--v-theme-primary));
}
.dash-sub {
  margin-top: 12px;
}
.dash-sub-card {
  padding: 16px;
  text-align: center;
}
.dash-sub-label {
  font-size: 13px;
  color: rgba(var(--v-theme-on-surface), 0.6);
  margin-bottom: 6px;
}
.dash-sub-num {
  font-size: 24px;
  font-weight: 700;
}
</style>
