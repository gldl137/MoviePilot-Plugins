<script setup>
import { ref, computed, onMounted } from 'vue'

const props = defineProps({
  api: { type: Object, default: () => ({}) },
  pluginId: { type: String, default: '' },
  navKey: { type: String, default: 'main' },
  sourcePluginId: { type: String, default: '' },
  nativeSubscribe: { type: Function, default: null },
})

const loading = ref(false)
const data = ref({
  records: [],
  total: 0,
  successCount: 0,
  failedCount: 0,
  enabled: false,
  scanning: false,
  last_scan: '',
  last_result: '',
})

// 分页（前端分页，后端 records 最多 200 条）
const page = ref(1)
const itemsPerPage = ref(50)
const perPageOptions = [
  { title: '10 条/页', value: 10 },
  { title: '20 条/页', value: 20 },
  { title: '50 条/页', value: 50 },
  { title: '100 条/页', value: 100 },
]
const totalPages = computed(() =>
  Math.max(1, Math.ceil(data.value.records.length / itemsPerPage.value)))
const pagedRecords = computed(() => {
  const start = (page.value - 1) * itemsPerPage.value
  return data.value.records.slice(start, start + itemsPerPage.value)
})
const rangeText = computed(() => {
  if (!data.value.records.length) return '0 条'
  const start = (page.value - 1) * itemsPerPage.value + 1
  const end = Math.min(page.value * itemsPerPage.value, data.value.records.length)
  return `${start}-${end} / ${data.value.records.length}`
})

function relativeTime(text) {
  if (!text) return ''
  const t = new Date(String(text).replace(/-/g, '/')).getTime()
  if (Number.isNaN(t)) return text
  const diff = Math.max(0, Date.now() - t)
  const min = Math.floor(diff / 60000)
  if (min < 1) return '刚刚'
  if (min < 60) return `${min}分钟前`
  const hour = Math.floor(min / 60)
  if (hour < 24) return `${hour}小时前`
  const day = Math.floor(hour / 24)
  if (day < 30) return `${day}天前`
  return text
}

function rowTitle(r) {
  return r.title || (String(r.filename || '').replace(/\.cas$/i, ''))
}
function rowNetPath(r) {
  return r.net_path || r.target || ''
}

async function load() {
  loading.value = true
  try {
    const r = await props.api.get(`plugin/${props.pluginId}/records`)
    if (r && typeof r === 'object') {
      data.value = {
        records: r.records || [],
        total: r.total || 0,
        successCount: r.successCount || 0,
        failedCount: r.failedCount || 0,
        enabled: !!r.enabled,
        scanning: !!r.scanning,
        last_scan: r.last_scan || '',
        last_result: r.last_result || '',
      }
      if (page.value > totalPages.value) page.value = 1
    }
  } catch (e) {
    console.error('CasRestore load records failed', e)
  } finally {
    loading.value = false
  }
}

async function action(endpoint) {
  try {
    await props.api.post(`plugin/${props.pluginId}/${endpoint}`)
  } catch (e) {
    console.error('CasRestore action failed', endpoint, e)
  }
  await load()
}

onMounted(load)
</script>

<template>
  <div class="pa-4">
    <!-- 头部：分组 + 标题 + 统计 + 操作按钮 -->
    <div class="d-flex align-start justify-space-between flex-wrap mb-3" style="gap: 8px">
      <div class="min-w-0">
        <div class="text-subtitle-2 text-primary">媒体整理</div>
        <div class="text-h6 mt-1">CAS 恢复</div>
        <div class="text-body-2 text-medium-emphasis mt-1">
          共 {{ data.total }} 条记录 · 成功 {{ data.successCount }} · 失败 {{ data.failedCount }}
          <template v-if="data.scanning"> · 扫描中…</template>
        </div>
        <div class="text-caption text-medium-emphasis mt-1">
          状态：{{ data.scanning ? '扫描中' : '空闲' }} · 启用={{ data.enabled }} · 上次扫描={{ data.last_scan || '从未' }}
          <template v-if="data.last_result"> · {{ data.last_result }}</template>
        </div>
      </div>
      <div class="d-flex align-center flex-wrap" style="gap: 8px">
        <v-btn size="small" variant="outlined" color="primary" :loading="loading" @click="action('scan')">
          立即扫描
        </v-btn>
        <v-btn size="small" variant="outlined" color="info" @click="action('test')">
          测试连接
        </v-btn>
        <v-btn size="small" variant="outlined" color="error" @click="action('clear_records')">
          清空记录
        </v-btn>
      </div>
    </div>

    <v-divider class="mb-1" />

    <!-- 记录列表（行式布局） -->
    <div v-if="!data.records.length" class="text-center text-medium-emphasis py-12">
      暂无恢复记录
    </div>
    <template v-else>
      <div
        v-for="(r, i) in pagedRecords"
        :key="`${page}-${i}`"
        class="record-row d-flex align-center"
      >
        <v-icon
          size="22"
          class="mr-3 flex-shrink-0"
          :color="r.status === '成功' ? 'success' : 'error'"
        >
          {{ r.status === '成功' ? 'mdi-check-circle-outline' : 'mdi-alert-circle-outline' }}
        </v-icon>

        <!-- 第一列：名字 -->
        <div class="record-name text-truncate" :title="rowTitle(r)">{{ rowTitle(r) }}</div>

        <!-- 第二列：上监控目录 / 下网盘目录 -->
        <div class="flex-grow-1 min-w-0">
          <div v-if="r.local_path" class="record-path text-truncate" :title="r.local_path">
            <span class="path-tag">监控</span>{{ r.local_path }}
          </div>
          <div v-if="rowNetPath(r)" class="record-path text-truncate" :title="rowNetPath(r)">
            <span class="path-tag path-tag-net">网盘</span>{{ rowNetPath(r) }}
          </div>
          <div v-if="!r.local_path && !rowNetPath(r) && r.message" class="record-path text-truncate">
            {{ r.message }}
          </div>
        </div>

        <div class="d-flex flex-column align-end flex-shrink-0 ml-3" style="gap: 3px">
          <div class="d-flex align-center" style="gap: 4px">
            <v-chip size="x-small" variant="tonal" :color="r.status === '成功' ? 'success' : 'error'">
              {{ r.status }}
            </v-chip>
            <v-chip v-if="r.mode" size="x-small" variant="tonal" color="primary">{{ r.mode }}</v-chip>
          </div>
          <div class="d-flex align-center" style="gap: 6px">
            <span v-if="r.size" class="record-meta">{{ r.size }}</span>
            <span class="record-meta">{{ relativeTime(r.time) }}</span>
          </div>
        </div>
      </div>

      <!-- 底部分页 -->
      <div class="d-flex align-center justify-space-between flex-wrap mt-4" style="gap: 8px">
        <v-select
          v-model="itemsPerPage"
          :items="perPageOptions"
          density="compact"
          variant="outlined"
          hide-details
          style="max-width: 130px"
          @update:model-value="page = 1"
        />
        <span class="text-body-2 text-medium-emphasis">{{ rangeText }}</span>
        <v-pagination
          v-model="page"
          :length="totalPages"
          :total-visible="7"
          density="comfortable"
        />
      </div>
    </template>
  </div>
</template>

<style scoped>
.record-row {
  padding: 10px 12px;
  border-radius: 8px;
  transition: background-color 0.15s ease;
}
.record-row:hover {
  background: rgba(var(--v-theme-on-surface), 0.04);
}
.record-row + .record-row {
  border-top: 1px solid rgba(var(--v-theme-on-surface), 0.06);
}
.record-title {
  font-size: 0.9rem;
  font-weight: 500;
}
.record-path {
  font-size: 0.75rem;
  color: rgba(var(--v-theme-on-surface), 0.55);
  margin-top: 3px;
  white-space: nowrap;
}
.record-name {
  width: 200px;
  flex-shrink: 0;
  font-size: 0.9rem;
  font-weight: 500;
  padding-right: 12px;
  white-space: nowrap;
}
.path-tag {
  display: inline-block;
  font-size: 0.65rem;
  line-height: 1.4;
  padding: 0 4px;
  margin-right: 5px;
  min-width: 32px;
  text-align: center;
  border-radius: 3px;
  background: rgba(var(--v-theme-on-surface), 0.09);
  color: rgba(var(--v-theme-on-surface), 0.7);
  vertical-align: middle;
}
.path-tag-net {
  background: rgba(var(--v-theme-primary), 0.14);
  color: rgb(var(--v-theme-primary));
}
.record-meta {
  font-size: 0.72rem;
  color: rgba(var(--v-theme-on-surface), 0.55);
  white-space: nowrap;
}
.min-w-0 {
  min-width: 0;
}
</style>
