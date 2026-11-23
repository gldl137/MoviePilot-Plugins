<script setup>
import { ref, onMounted, watch } from 'vue'

const props = defineProps({
  api: { type: Object, default: () => ({}) },
  pluginId: { type: String, default: '' },
})

const cfg = ref({
  enabled: false,
  watch_dir: '',
  account_id: '',
  scope: 'personal',
  family_id: '',
  target_folder_path: '',
  notify: true,
  onlyonce: false,
})

const accounts = ref([])
const families = ref([])
const accountHint = ref('')
const familyHint = ref('')
const loading = ref(false)
const saving = ref(false)

async function loadConfig() {
  try {
    const r = await props.api.get(`plugin/${props.pluginId}/config`)
    if (r && r.config && typeof r.config === 'object') {
      cfg.value = { ...cfg.value, ...r.config }
    }
  } catch (e) {
    console.error('CasRestore load config failed', e)
  }
}

async function loadAccounts() {
  try {
    const r = await props.api.get(`plugin/${props.pluginId}/accounts`)
    if (r && r.success) {
      accounts.value = r.items || []
      accountHint.value = `共 ${accounts.value.length} 个账号`
      if (!cfg.value.account_id && accounts.value.length) {
        cfg.value.account_id = accounts.value[0].value
      }
    } else {
      accounts.value = []
      accountHint.value = (r && r.message) || '拉取失败'
    }
  } catch (e) {
    accountHint.value = '拉取失败：' + (e && e.message ? e.message : e)
  }
}

async function loadFamilies() {
  if (!cfg.value.account_id || cfg.value.scope !== 'family') {
    families.value = []
    familyHint.value = cfg.value.scope !== 'family' ? '个人空间无需选择家庭' : '先选择账号'
    return
  }
  try {
    const r = await props.api.get(`plugin/${props.pluginId}/families`, {
      params: { account_id: cfg.value.account_id },
    })
    if (r && r.success) {
      families.value = r.items || []
      familyHint.value = `共 ${families.value.length} 个家庭`
    } else {
      families.value = []
      familyHint.value = (r && r.message) || '拉取失败'
    }
  } catch (e) {
    families.value = []
    familyHint.value = '拉取失败：' + (e && e.message ? e.message : e)
  }
}

async function save() {
  saving.value = true
  try {
    await props.api.post(`plugin/${props.pluginId}/save_config`, cfg.value)
  } catch (e) {
    console.error('CasRestore save config failed', e)
  } finally {
    saving.value = false
  }
}

watch(() => cfg.value.account_id, loadFamilies)
watch(() => cfg.value.scope, loadFamilies)

onMounted(async () => {
  loading.value = true
  try {
    await loadConfig()
    await loadAccounts()
    await loadFamilies()
  } finally {
    loading.value = false
  }
})
</script>

<template>
  <div class="pa-4">
    <v-card>
      <v-card-title>CAS 恢复源文件 · 设置</v-card-title>
      <v-card-text>
        <v-alert type="info" variant="tonal" density="compact" class="mb-4">
          监控目录有新 .cas 写入时自动恢复；CAS 服务地址与 API Key 读取「CAS助手」插件，账号 / 家庭 / 目标路径在此选择。
        </v-alert>
        <v-row>
          <v-col cols="12">
            <v-switch v-model="cfg.enabled" label="启用插件" density="compact" hide-details />
          </v-col>
          <v-col cols="12">
            <v-text-field
              v-model="cfg.watch_dir"
              label="监控目录"
              hint="容器内绝对路径（含子目录）"
              persistent-hint
              density="compact"
            />
          </v-col>
          <v-col cols="12" md="4">
            <v-select
              v-model="cfg.account_id"
              :items="accounts"
              label="恢复账号"
              :hint="accountHint"
              persistent-hint
              density="compact"
              item-title="label"
              item-value="value"
            />
          </v-col>
          <v-col cols="12" md="4">
            <v-select
              v-model="cfg.scope"
              :items="[
                { title: '个人空间', value: 'personal' },
                { title: '家庭空间', value: 'family' },
              ]"
              label="恢复位置"
              item-title="title"
              item-value="value"
              density="compact"
              hide-details
            />
          </v-col>
          <v-col cols="12" md="4">
            <v-select
              v-model="cfg.family_id"
              :items="families"
              label="家庭列表"
              :hint="familyHint"
              persistent-hint
              density="compact"
              item-title="label"
              item-value="value"
            />
          </v-col>
          <v-col cols="12">
            <v-text-field
              v-model="cfg.target_folder_path"
              label="目标文件夹路径"
              hint="网盘基路径（如 家庭1/测试），本地子目录自动拼接到其后"
              persistent-hint
              density="compact"
            />
          </v-col>
          <v-col cols="12" md="6">
            <v-switch v-model="cfg.notify" label="发送通知" density="compact" hide-details />
          </v-col>
          <v-col cols="12" md="6">
            <v-switch v-model="cfg.onlyonce" label="立即运行一次" density="compact" hide-details />
          </v-col>
        </v-row>
      </v-card-text>
      <v-card-actions>
        <v-spacer />
        <v-btn color="primary" :loading="saving" @click="save">保存</v-btn>
      </v-card-actions>
    </v-card>
  </div>
</template>
