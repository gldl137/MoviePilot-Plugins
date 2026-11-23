<script setup>
import { ref, onMounted } from 'vue'

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
    <v-card>
      <v-card-title class="d-flex align-center justify-space-between">
        <div>
          CAS 恢复（共 {{ data.total }} 条 · 成功 {{ data.successCount }} · 失败 {{ data.failedCount }}）
        </div>
        <div class="d-flex align-center" style="gap: 8px">
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
      </v-card-title>
      <v-card-text>
        <div class="text-body-2 mb-2 text-medium-emphasis">
          状态：{{ data.scanning ? '扫描中' : '空闲' }} · 启用={{ data.enabled }} · 上次扫描={{ data.last_scan || '从未' }}
          <template v-if="data.last_result"> · {{ data.last_result }}</template>
        </div>
        <v-table density="comfortable" hover fixed-header height="600px">
          <thead>
            <tr>
              <th class="text-no-wrap" style="width: 170px">时间</th>
              <th>本地文件</th>
              <th>恢复到网盘</th>
              <th style="width: 90px">状态</th>
              <th>说明</th>
            </tr>
          </thead>
          <tbody>
            <tr v-if="!data.records.length">
              <td colspan="5" class="text-center text-medium-emphasis">暂无恢复记录</td>
            </tr>
            <tr v-for="(r, i) in data.records" :key="i">
              <td class="text-no-wrap">{{ r.time }}</td>
              <td>{{ r.filename }}</td>
              <td>{{ r.target }}</td>
              <td>
                <v-chip size="small" :color="r.status === '成功' ? 'success' : 'error'">
                  {{ r.status }}
                </v-chip>
              </td>
              <td>{{ r.message }}</td>
            </tr>
          </tbody>
        </v-table>
      </v-card-text>
    </v-card>
  </div>
</template>
