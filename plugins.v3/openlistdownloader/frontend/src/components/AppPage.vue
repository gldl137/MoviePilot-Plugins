<script setup lang="ts">
import { ref, computed, onMounted, watch } from 'vue'
import { useDisplay } from 'vuetify'

const display = useDisplay()
const isMobile = computed(() => display.mdAndDown.value)

const props = defineProps({
  navKey: { type: String, default: 'main' },
  initialConfig: { type: Object, default: () => ({}) },
  api: { type: Object, default: () => ({}) },
  pluginId: { type: String, default: '' },
})

const PLUGIN_ID = 'OpenListDownloader'

// 列表状态
const loading = ref(false)
const items = ref<any[]>([])
const error = ref('')

// 搜索 & 分页
const search = ref('')
const page = ref(1)
const itemsPerPage = ref(50)

// 选中的行
const selected = ref<any[]>([])

const headers = [
  { title: '标题', key: 'title', sortable: false },
  { title: '文件', key: 'file', sortable: false },
  { title: '来源', key: 'path', sortable: false },
  { title: '大小', key: 'size', sortable: false },
  { title: '时间', key: 'time', sortable: false },
  { title: '状态', key: 'status', sortable: false },
  { title: '操作', key: 'actions', sortable: false },
]

function humanSize(bytes?: number): string {
  if (!bytes || bytes <= 0) return '-'
  const units = ['B', 'KB', 'MB', 'GB', 'TB']
  let size = Number(bytes)
  let i = 0
  while (size >= 1024 && i < units.length - 1) {
    size /= 1024
    i++
  }
  return `${size.toFixed(i === 0 ? 0 : 2)} ${units[i]}`
}

// 将下载链接解码为可读路径：去掉 http(s)://host:port/d/ 前缀并还原 URL 编码的中文
function cleanPath(url?: string): string {
  if (!url) return ''
  let u = url
  try {
    u = decodeURIComponent(u)
  } catch (e) {
    /* 保留原始值 */
  }
  return u.replace(/^https?:\/\/[^/]+\/d\//, '')
}

async function loadHistory() {
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
    items.value = (list || []).map((h: any, idx: number) => ({
      id: idx,
      poster: h.poster || '',
      title: h.title || '未知',
      year: h.year || '',
      season: h.season || '',
      name: h.name || '',
      download_url: h.download_url || '',
      path_display: cleanPath(h.download_url) || h.name || '',
      media_type: h.media_type || h.type || '文件',
      tag: h.media_type || h.type || '',
      size: humanSize(h.size),
      size_bytes: Number(h.size) || 0,
      time: h.time || '',
      status: h.status || '成功',
    }))
  } catch (e: any) {
    error.value = String(e?.message || e)
  } finally {
    loading.value = false
  }
}

onMounted(loadHistory)
watch(search, () => { page.value = 1 })

// 操作提示
const notice = ref('')
const noticeType = ref<'success' | 'error'>('success')
const noticeVisible = ref(false)
let noticeTimer: any = null
function showNotice(msg: string, type: 'success' | 'error' = 'success') {
  notice.value = msg
  noticeType.value = type
  noticeVisible.value = true
  if (noticeTimer) clearTimeout(noticeTimer)
  noticeTimer = setTimeout(() => { noticeVisible.value = false }, 2500)
}

async function callPost(path: string, body: any): Promise<any> {
  if (props.api && typeof props.api.post === 'function') {
    return await props.api.post(`plugin/${PLUGIN_ID}/${path}`, body)
  }
  const res = await fetch(`/api/v1/plugin/${PLUGIN_ID}/${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
  return await res.json()
}

async function redownload(it: any) {
  try {
    const res = await callPost('redownload', { name: it.name })
    if (res?.success === false) {
      showNotice(res?.message || '重新下载失败', 'error')
    } else {
      showNotice('已重新提交下载')
    }
  } catch (e: any) {
    showNotice(String(e?.message || e), 'error')
  }
}

async function deleteRecord(it: any) {
  try {
    const res = await callPost('history/delete', { name: it.name })
    if (res?.success === false) {
      showNotice(res?.message || '删除失败', 'error')
    } else {
      items.value = items.value.filter(i => i.name !== it.name)
      showNotice('已删除该记录')
    }
  } catch (e: any) {
    showNotice(String(e?.message || e), 'error')
  }
}

const filtered = computed(() => {
  const kw = search.value.trim().toLowerCase()
  if (!kw) return items.value
  return items.value.filter((it: any) => {
    return (it.title || '').toLowerCase().includes(kw)
      || (it.name || '').toLowerCase().includes(kw)
      || (it.media_type || '').toLowerCase().includes(kw)
      || (it.tag || '').toLowerCase().includes(kw)
  })
})

const totalPages = computed(() => Math.max(1, Math.ceil(filtered.value.length / itemsPerPage.value)))

const pagedItems = computed(() => {
  const start = (page.value - 1) * itemsPerPage.value
  return filtered.value.slice(start, start + itemsPerPage.value)
})

const rangeText = computed(() => {
  if (!filtered.value.length) return '0 - 0 / 0'
  const start = (page.value - 1) * itemsPerPage.value + 1
  const end = Math.min(start + itemsPerPage.value - 1, filtered.value.length)
  return `${start} - ${end} / ${filtered.value.length}`
})
</script>

<template>
  <div class="dawn-page">
    <!-- 顶部：刷新 -->
    <div class="dawn-topbar">
      <v-btn variant="tonal" size="small" prepend-icon="mdi-refresh" @click="loadHistory">
        刷新
      </v-btn>
    </div>

    <v-alert v-if="error" type="error" variant="tonal" class="my-3">{{ error }}</v-alert>

    <v-progress-linear v-if="loading" indeterminate color="primary" />

    <v-card variant="flat" class="mt-2">
      <div v-if="!filtered.length && !loading" class="text-medium-emphasis pa-6 text-center">
        暂无下载记录
      </div>

      <!-- 桌面端表格 -->
      <div v-if="!isMobile">
      <v-table density="comfortable" class="dawn-table">
        <thead>
          <tr>
            <th
              v-for="h in headers"
              :key="h.key"
              :style="h.width ? { width: h.width + 'px' } : undefined"
            >
              {{ h.title }}
            </th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="it in pagedItems" :key="it.id">
            <!-- 标题：封面 + 标题 + 季集 -->
            <td>
              <div class="dawn-title-cell">
                <div class="dawn-poster">
                  <v-img v-if="it.poster" :src="it.poster" cover>
                    <template #placeholder>
                      <div class="dawn-poster__ph">
                        <v-icon icon="mdi-image-off" size="18" />
                      </div>
                    </template>
                  </v-img>
                  <div v-else class="dawn-poster__ph">
                    <v-icon icon="mdi-image-off" size="18" />
                  </div>
                </div>
                <div class="dawn-title-text">
                  <div class="dawn-title-name">{{ it.title }}</div>
                  <div v-if="it.season" class="dawn-title-sub">+ 第{{ it.season }}季</div>
                </div>
              </div>
            </td>

            <!-- 文件：文件名 -->
            <td>
              <div class="dawn-file-cell">{{ it.name }}</div>
            </td>

            <!-- 路径：OpenList 下载链接 + 紫色 chip -->
            <td>
              <div class="dawn-path-cell">
                <div class="dawn-path-text">
                  <v-icon icon="mdi-folder-outline" size="14" class="mr-1" />
                  {{ it.path_display }}
                </div>
                <v-chip v-if="it.tag" size="x-small" color="purple" variant="tonal" label class="mt-1">
                  {{ it.tag }}
                </v-chip>
              </div>
            </td>

            <td>{{ it.size }}</td>
            <td>{{ it.time }}</td>

            <td>
              <v-chip size="x-small" color="success" variant="flat" label>
                {{ it.status }}
              </v-chip>
            </td>

            <!-- 操作菜单 -->
            <td>
              <v-menu>
                <template #activator="{ props: a }">
                  <v-btn icon="mdi-dots-vertical" variant="text" size="small" v-bind="a" />
                </template>
                <v-list density="compact">
                  <v-list-item prepend-icon="mdi-redo" title="重新下载" @click="redownload(it)" />
                  <v-list-item prepend-icon="mdi-delete-outline" title="删除记录" @click="deleteRecord(it)" />
                </v-list>
              </v-menu>
            </td>
          </tr>
        </tbody>
      </v-table>

      <!-- 底部分页 -->
      <div class="dawn-footer">
        <div class="dawn-footer__total">{{ rangeText }}</div>
        <div class="dawn-footer__pager">
          <v-btn
            icon="mdi-chevron-left"
            variant="text"
            size="small"
            :disabled="page <= 1"
            @click="page--"
          />
          <template v-for="p in totalPages" :key="p">
            <v-btn
              v-if="p === 1 || p === totalPages || Math.abs(p - page) <= 2"
              :variant="p === page ? 'flat' : 'text'"
              :color="p === page ? 'primary' : undefined"
              size="small"
              @click="page = p"
            >
              {{ p }}
            </v-btn>
            <span v-else-if="(p === 2 && page > 4) || (p === totalPages - 1 && page < totalPages - 3)" class="dawn-ellipsis">…</span>
          </template>
          <v-btn
            icon="mdi-chevron-right"
            variant="text"
            size="small"
            :disabled="page >= totalPages"
            @click="page++"
          />
        </div>
      </div>
      </div>

      <!-- 移动端卡片列表 -->
      <div v-else-if="isMobile" class="dawn-mobile">
        <v-card
          v-for="it in pagedItems"
          :key="it.id"
          variant="flat"
          class="dawn-mobile-card mb-3"
        >
          <div class="dawn-mobile-header">
            <div class="dawn-mobile-poster">
              <v-img v-if="it.poster" :src="it.poster" cover>
                <template #placeholder>
                  <div class="dawn-poster__ph">
                    <v-icon icon="mdi-image-off" size="20" />
                  </div>
                </template>
              </v-img>
              <div v-else class="dawn-poster__ph">
                <v-icon icon="mdi-image-off" size="20" />
              </div>
              <div class="dawn-mobile-poster__overlay">
                <v-icon icon="mdi-play" size="22" color="white" />
              </div>
            </div>
            <div class="dawn-mobile-titles">
              <div class="dawn-mobile-title">{{ it.title }}</div>
              <div v-if="it.name !== it.title" class="dawn-mobile-sub">{{ it.name }}</div>
              <div class="dawn-mobile-meta mt-1">
                <v-chip
                  v-if="it.tag"
                  size="x-small"
                  color="purple"
                  variant="tonal"
                  label
                >
                  {{ it.tag }}
                </v-chip>
                <span class="dawn-mobile-size">{{ it.size }}</span>
                <span class="dawn-mobile-dot">·</span>
                <span class="dawn-mobile-time">{{ it.time }}</span>
              </div>
            </div>
            <div class="dawn-mobile-right">
              <v-chip size="x-small" color="success" variant="flat" label>
                {{ it.status }}
              </v-chip>
              <v-menu>
                <template #activator="{ props: a }">
                  <v-btn icon="mdi-dots-vertical" variant="text" size="small" v-bind="a" />
                </template>
                <v-list density="compact">
                  <v-list-item prepend-icon="mdi-redo" title="重新下载" @click="redownload(it)" />
                  <v-list-item prepend-icon="mdi-delete-outline" title="删除记录" @click="deleteRecord(it)" />
                </v-list>
              </v-menu>
            </div>
          </div>
          <!-- 路径列表 -->
          <div v-if="it.download_url || it.name" class="dawn-mobile-paths">
            <div v-if="it.name && it.download_url" class="dawn-mobile-path-row">
              <span class="dawn-mobile-path-tag">文件</span>
              <div class="dawn-path-text">{{ it.name }}</div>
            </div>
            <div class="dawn-mobile-path-row">
              <span class="dawn-mobile-path-tag">来源</span>
              <div class="dawn-path-text">
                <v-icon icon="mdi-folder-outline" size="14" class="mr-1" />
                {{ it.path_display }}
              </div>
            </div>
          </div>
        </v-card>
        <!-- 移动端分页 -->
        <div class="dawn-footer">
          <div class="dawn-footer__total">{{ rangeText }}</div>
          <div class="dawn-footer__pager">
            <v-btn
              icon="mdi-chevron-left"
              variant="text"
              size="small"
              :disabled="page <= 1"
              @click="page--"
            />
            <span class="dawn-mobile-page">{{ page }} / {{ totalPages }}</span>
            <v-btn
              icon="mdi-chevron-right"
              variant="text"
              size="small"
              :disabled="page >= totalPages"
              @click="page++"
            />
          </div>
        </div>
      </div>
    </v-card>

    <!-- 操作结果提示 -->
    <v-snackbar
      v-model="noticeVisible"
      :color="noticeType"
      location="bottom"
      timeout="2500"
    >
      {{ notice }}
    </v-snackbar>
  </div>
</template>

<style scoped>
.dawn-page {
  padding: 12px;
}
.dawn-topbar {
  display: flex;
  align-items: center;
  justify-content: flex-end;
  margin-bottom: 8px;
}
.dawn-table :deep(td),
.dawn-table :deep(th) {
  vertical-align: middle;
}
.dawn-title-cell {
  display: flex;
  align-items: center;
  min-height: 56px;
}
.dawn-poster {
  width: 38px;
  height: 56px;
  flex-shrink: 0;
  margin-right: 10px;
  border-radius: 4px;
  overflow: hidden;
  background: rgba(var(--v-theme-on-surface), 0.06);
}
.dawn-poster__ph {
  width: 100%;
  height: 100%;
  display: flex;
  align-items: center;
  justify-content: center;
  color: rgba(var(--v-theme-on-surface), 0.4);
}
.dawn-title-text {
  display: flex;
  flex-direction: column;
}
.dawn-title-name {
  font-weight: 600;
  font-size: 13px;
  line-height: 1.2;
}
.dawn-title-sub {
  font-size: 12px;
  color: rgba(var(--v-theme-on-surface), 0.6);
  margin-top: 2px;
}
.dawn-file-cell {
  max-width: 260px;
  font-size: 13px;
  color: rgba(var(--v-theme-on-surface), 0.85);
  word-break: break-all;
  line-height: 1.3;
}
.dawn-path-cell {
  max-width: 420px;
}
.dawn-path-link {
  font-size: 12px;
  color: rgb(var(--v-theme-primary));
  text-decoration: none;
  word-break: break-all;
  line-height: 1.4;
  display: inline-flex;
  align-items: flex-start;
}
.dawn-path-link:hover {
  text-decoration: underline;
}
.dawn-path-text {
  font-size: 12px;
  color: rgba(var(--v-theme-on-surface), 0.7);
  word-break: break-all;
  line-height: 1.3;
}
.dawn-footer {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 6px 12px;
  border-top: 1px solid rgba(var(--v-theme-on-surface), 0.08);
}
.dawn-footer__total {
  font-size: 13px;
  color: rgba(var(--v-theme-on-surface), 0.6);
}
.dawn-footer__pager {
  display: flex;
  align-items: center;
  gap: 4px;
}
.dawn-ellipsis {
  padding: 0 6px;
  color: rgba(var(--v-theme-on-surface), 0.6);
}

/* 移动端卡片列表 */
.dawn-mobile {
  padding-top: 4px;
}
.dawn-mobile-card {
  padding: 12px;
}
.dawn-mobile-header {
  display: flex;
  align-items: flex-start;
}
.dawn-mobile-poster {
  position: relative;
  width: 64px;
  height: 90px;
  flex-shrink: 0;
  margin-right: 12px;
  border-radius: 4px;
  overflow: hidden;
  background: rgba(var(--v-theme-on-surface), 0.06);
}
.dawn-mobile-poster :deep(.v-img__img--cover) {
  object-fit: cover;
}
.dawn-mobile-poster__overlay {
  position: absolute;
  inset: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  background: rgba(0, 0, 0, 0.35);
  pointer-events: none;
}
.dawn-mobile-titles {
  flex: 1;
  min-width: 0;
}
.dawn-mobile-title {
  font-weight: 700;
  font-size: 15px;
  line-height: 1.25;
}
.dawn-mobile-sub {
  font-size: 12px;
  color: rgba(var(--v-theme-on-surface), 0.6);
  margin-top: 2px;
}
.dawn-mobile-meta {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 6px;
  font-size: 12px;
  color: rgba(var(--v-theme-on-surface), 0.7);
}
.dawn-mobile-size,
.dawn-mobile-time {
  white-space: nowrap;
}
.dawn-mobile-dot {
  opacity: 0.5;
}
.dawn-mobile-right {
  display: flex;
  flex-direction: column;
  align-items: flex-end;
  gap: 4px;
  margin-left: 4px;
}
.dawn-mobile-paths {
  margin-top: 10px;
  display: flex;
  flex-direction: column;
  gap: 6px;
}
.dawn-mobile-path-row {
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 12px;
}
.dawn-mobile-path-tag {
  flex-shrink: 0;
  padding: 2px 10px;
  border-radius: 999px;
  background: rgba(var(--v-theme-primary), 0.08);
  color: rgb(var(--v-theme-primary));
  font-size: 11px;
  font-weight: 600;
}
.dawn-mobile-page {
  font-size: 13px;
  padding: 0 8px;
  color: rgba(var(--v-theme-on-surface), 0.7);
}
</style>