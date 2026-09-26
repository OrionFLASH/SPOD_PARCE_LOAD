/* game_fill_io.js — импорт/экспорт JSON и CSV, localStorage */
"use strict";
function buildTables(srcList){
  const tables={contest:[],reward:[],reward_link:[],group:[],indicator:[],schedule:[]};
  const seen=new Set();
  const src=srcList||contests;
  const exportStands=CSV_EXPORT_STAND_PROM;
  for(const c of src){
    if(!contestIncludedInExport(c, exportStands)) continue;
    const d=c.data;
    tables.contest.push(contestRow(d));
    const badges=(d.badges||[]).filter((b,i)=>rowIncludedInExport(b,c,exportStands)||rowIncludedInExport((d.reward_link||[])[i],c,exportStands));
    for(const r of rewardRows({...d,badges})){const code=r.REWARD_CODE||"";if(code&&seen.has(code))continue;if(code)seen.add(code);tables.reward.push(r)}
    tables.reward_link.push(...tableRows((d.reward_link||[]).filter(r=>rowIncludedInExport(r,c,exportStands)),LINK_COLS));
    tables.group.push(...tableRows((d.group||[]).filter(r=>rowIncludedInExport(r,c,exportStands)),GROUP_COLS));
    tables.indicator.push(...indicatorRows({...d,indicator:(d.indicator||[]).filter(r=>rowIncludedInExport(r,c,exportStands))}));
    tables.schedule.push(...scheduleRows({...d,schedule:(d.schedule||[]).filter(r=>rowIncludedInExport(r,c,exportStands))}));
  }
  return tables;
}
function resolveExportList(){
  const st=exportSelectionState();
  if(st.mode==="empty"){toast("Ничего не выбрано");return null}
  return st;
}
function exportOne(key,ts){
  const st=resolveExportList();
  if(!st) return;
  const tables=buildTables(st.list);
  const cols=exportCols()[key];
  const name=fileName(key,ts);
  downloadCsv(name,toCsv(tables[key],cols));
  toast("CSV: "+name+(st.mode==="partial"?" · выбранные":"")+(archiveEntries.length&&st.mode==="all"?" · без архива":""));
}
async function exportAll(){
  const st=resolveExportList();
  if(!st) return;
  const ts=exportStamp();
  const tables=buildTables(st.list);
  const cols=exportCols();
  const archNote=(archiveEntries.length&&st.mode==="all")?" · без архива":(st.mode==="partial"?" · выбранные":"");
  // Chromium: одна папка — все 6 файлов без блокировки множественных download
  if(typeof window.showDirectoryPicker==="function"){
    try{
      const dir=await window.showDirectoryPicker({mode:"readwrite",id:"spod-fill-csv"});
      for(const k of EXPORT_KEYS){
        await writeCsvToDirectory(dir,fileName(k,ts),toCsv(tables[k],cols[k]));
      }
      toast("Все 6 CSV в выбранную папку"+archNote+" · "+ts);
      return;
    }catch(err){
      if(err&&err.name==="AbortError"){toast("Экспорт отменён");return}
      // нет доступа к FS API — fallback на скачивания
    }
  }
  // Fallback: пауза между файлами + отложенный revoke (иначе часто пропадает REWARD)
  for(const k of EXPORT_KEYS){
    await downloadCsv(fileName(k,ts),toCsv(tables[k],cols[k]));
    await new Promise(r=>setTimeout(r,450));
  }
  toast("Все 6 CSV (UTF-8 BOM · ;)"+archNote+" · "+ts+" · если какого-то нет — разрешите множественные загрузки в браузере");
}

function catalogStamp(data){return String((data&&(data.exported_at||data.generated_at||data.version))||"")}
async function loadCatalogPreferFile(){
  try{
    const url=new URL(CATALOG_URL, window.location.href).href;
    const res=await fetch(url,{cache:"no-store"});
    if(!res.ok) throw new Error("HTTP "+res.status);
    const data=await res.json();
    if(!data||!Array.isArray(data.sections)) throw new Error("нет sections");
    return {data, source:"catalog.json"};
  }catch(_){
    return {data:EMBEDDED_CATALOG, source:"embedded"};
  }
}
var sessionReady=false;
var sidebarOpen=true;
var filtersOpen=true;

function setSidebarOpen(open){
  sidebarOpen=!!open;
  const app=document.querySelector(".app");
  if(app) app.classList.toggle("is-sidebar-collapsed",!sidebarOpen);
  const hide=$("btn-sidebar-hide");
  const show=$("btn-sidebar-show");
  if(hide) hide.setAttribute("aria-expanded",sidebarOpen?"true":"false");
  if(show) show.setAttribute("aria-expanded",sidebarOpen?"true":"false");
}
function setFiltersOpen(open){
  filtersOpen=!!open;
  const app=document.querySelector(".app");
  if(app) app.classList.toggle("is-filters-collapsed",!filtersOpen);
  const hide=$("btn-filters-hide");
  const show=$("btn-filters-show");
  if(hide) hide.setAttribute("aria-expanded",filtersOpen?"true":"false");
  if(show) show.setAttribute("aria-expanded",filtersOpen?"true":"false");
}
function remountContestListDateFilter(){
  const host=$("filter-date-host");
  if(!host) return;
  host.innerHTML="";
  mountDateUi(host, contestListDate, (v)=>{
    contestListDate=String(v||"");
    const clear=$("filter-date-clear");
    if(clear) clear.hidden=!isIsoDate(contestListDate);
    renderContestTabs();
  }, "Дата внутри периода START_DT…END_DT");
  const clear=$("filter-date-clear");
  if(clear) clear.hidden=!isIsoDate(contestListDate);
}
var catalogSource="embedded";

/** Версия снимка: contests = активные; archive = только архив (в CSV не идёт). */
var PROJECT_JSON_VERSION=5;

function normalizeArchiveBundle(raw){
  if(!raw||typeof raw!=="object") return null;
  if(String(raw.kind||"")!=="bundle") return null;
  const e=clone(raw);
  if(!e.id) e.id=newArchiveId();
  e.kind="bundle";
  e.contestId=String(e.contestId||"");
  e.contestCode=String(e.contestCode||"").trim();
  e.contestName=String(e.contestName||"").trim();
  e.label=String(e.label||e.contestCode||e.contestName||"Конкурс");
  if(!e.deletedAt) e.deletedAt=new Date().toISOString();
  e.whole=!!e.whole;
  if(e.whole){
    const snap=(e.snapshot&&typeof e.snapshot==="object")?e.snapshot:{};
    const data=(snap.data&&typeof snap.data==="object")?clone(snap.data):{contest:{},feature:{},badges:[],reward_link:[],group:[],indicator:[],schedule:[]};
    e.snapshot={id:String(snap.id||e.contestId||""),name:String(snap.name||e.contestName||""),data};
    e.fragments=[];
  }else{
    e.snapshot=null;
    e.fragments=Array.isArray(e.fragments)?e.fragments.map(f=>{
      if(!f||typeof f!=="object") return null;
      const x=clone(f);
      if(!x.id) x.id=newFragmentId();
      x.kind=String(x.kind||"").trim();
      if(!x.kind) return null;
      if(!x.deletedAt) x.deletedAt=e.deletedAt;
      if(!x.label) x.label=archiveKindLabel(x.kind);
      if(x.payload==null||typeof x.payload!=="object") x.payload={};
      return x;
    }).filter(Boolean):[];
  }
  return e;
}
/** Старые записи contest/group/… → бандлы по конкурсу. */
function migrateArchiveToBundles(list){
  const out=[];
  const map=new Map();
  function keyOf(e){
    return archiveBundleKey(e.contestId, e.contestCode) || ("orphan:"+e.id);
  }
  function ensure(e){
    const k=keyOf(e);
    let b=map.get(k);
    if(!b){
      b=emptyArchiveBundle({
        contestId:e.contestId,
        contestCode:e.contestCode,
        contestName:e.contestName,
        label:e.contestCode||e.contestName||e.label||"Конкурс",
      });
      b.id=e.kind==="bundle"?e.id:newArchiveId();
      map.set(k,b);
      out.push(b);
    }
    return b;
  }
  for(const raw of list||[]){
    if(!raw||typeof raw!=="object") continue;
    const kind=String(raw.kind||"").trim();
    if(kind==="bundle"){
      const b=normalizeArchiveBundle(raw);
      if(b){
        const k=keyOf(b);
        if(map.has(k)){
          // слить
          const cur=map.get(k);
          if(b.whole&&b.snapshot){cur.whole=true;cur.snapshot=b.snapshot;cur.fragments=[]}
          else for(const f of (b.fragments||[])) cur.fragments.push(f);
          if(b.deletedAt&&b.deletedAt>cur.deletedAt) cur.deletedAt=b.deletedAt;
        }else{map.set(k,b);out.push(b)}
      }
      continue;
    }
    if(kind==="contest"){
      const b=ensure(raw);
      b.whole=true;
      const snap=(raw.snapshot&&typeof raw.snapshot==="object")?raw.snapshot:{};
      const data=(snap.data&&typeof snap.data==="object")?clone(snap.data):(raw.data?clone(raw.data):null);
      b.snapshot={id:String(snap.id||raw.contestId||""),name:String(snap.name||raw.contestName||""),data:data||{contest:{},feature:{},badges:[],reward_link:[],group:[],indicator:[],schedule:[]}};
      // прежние фрагменты вливаем в снимок
      if((b.fragments||[]).length){
        b.snapshot.data=mergeFragmentsIntoSnapshotData(b.snapshot.data, b.fragments);
        b.fragments=[];
      }
      b.deletedAt=raw.deletedAt||b.deletedAt;
      touchArchiveBundle(b);
      continue;
    }
    if(["group","indicator","schedule","pair"].includes(kind)){
      const b=ensure(raw);
      if(b.whole&&b.snapshot&&b.snapshot.data){
        const d=b.snapshot.data;
        if(kind==="group"){if(!d.group)d.group=[];d.group.push(clone(raw.payload||{}))}
        else if(kind==="indicator"){if(!d.indicator)d.indicator=[];d.indicator.push(clone(raw.payload||{}))}
        else if(kind==="schedule"){if(!d.schedule)d.schedule=[];d.schedule.push(clone(raw.payload||{}))}
        else if(kind==="pair"){
          if(!d.badges)d.badges=[];if(!d.reward_link)d.reward_link=[];
          const p=raw.payload||{};
          d.badges.push(clone(p.badge||emptyBadge()));
          d.reward_link.push(clone(p.link||{}));
        }
      }else{
        pushArchiveFragment(b, kind, raw.payload||{}, raw.label||archiveKindLabel(kind), raw.detail||"");
      }
      if(raw.deletedAt&&raw.deletedAt>(b.deletedAt||"")) b.deletedAt=raw.deletedAt;
      touchArchiveBundle(b);
    }
  }
  return out;
}
/** Читает archive из снимка; мигрирует старый формат в бандлы. */
function readArchiveFromProject(restored){
  if(!restored||typeof restored!=="object") return [];
  let raw=restored.archive;
  if(raw&&typeof raw==="object"&&!Array.isArray(raw)&&Array.isArray(raw.entries)) raw=raw.entries;
  if(!Array.isArray(raw)) return [];
  return migrateArchiveToBundles(raw);
}
function serializeContestList(list){
  return (list||contests).map(c=>({id:c.id,name:contestTitle(c,0),stands:contestItemStands(c),data:clone(c.data)}));
}
function serializeActiveContests(){
  return serializeContestList(contests);
}
function serializeArchiveEntries(){
  return (archiveEntries||[]).map(normalizeArchiveBundle).filter(Boolean);
}
/**
 * Снимок проекта:
 * - contests / activeContest — основная (активная) часть
 * - archive — архивная часть отдельно; не смешивается с contests
 * CSV строится только из contests.
 * opts.contests — подмножество для выгрузки; opts.includeArchive=false без архива.
 */
function projectPayload(opts){
  opts=opts||{};
  const list=opts.contests||contests;
  const includeArchive=opts.includeArchive!==false;
  let active=activeContest;
  if(opts.contests){
    const curId=contests[activeContest]&&contests[activeContest].id;
    const ni=list.findIndex(c=>c.id===curId);
    active=ni>=0?ni:0;
  }
  return{
    version:PROJECT_JSON_VERSION,
    block:projectBlock||BLOCK,
    standsManifest:STANDS_UI.slice(),
    saved_at:new Date().toISOString(),
    catalog_stamp:catalogStamp(catalog),
    activeContest:active,
    contests:serializeContestList(list),
    archive:includeArchive?serializeArchiveEntries():[],
  };
}
function persistLocal(){
  if(!sessionReady)return;
  try{localStorage.setItem(LS_PROJECT,JSON.stringify(projectPayload()))}catch(_){}
}
function saveProjectFile(){
  if(!sessionReady||!contests.length){toast("Сначала создайте или откройте конкурс");return}
  const st=resolveExportList();
  if(!st) return;
  const payload=projectPayload({contests:st.list, includeArchive:st.mode==="all"});
  const text=JSON.stringify(payload,null,2)+"\n";
  const blob=new Blob([text],{type:"application/json;charset=utf-8"});
  const a=document.createElement("a");
  const stamp=new Date().toISOString().slice(0,16).replace(/[-:T]/g,"");
  a.href=URL.createObjectURL(blob);a.download=`spod_fill_${stamp}.json`;a.click();URL.revokeObjectURL(a.href);
  const nArch=(payload.archive||[]).length;
  toast("JSON сохранён: "+payload.contests.length+" конкурс."+(nArch?", архив: "+nArch:"")+(st.mode==="partial"?" · выбранные":"")+" · CSV архив не включает");
}
function peekDraft(){
  let restored=null;try{restored=JSON.parse(localStorage.getItem(LS_PROJECT)||"null")}catch(_){}
  if(!restored||!Array.isArray(restored.contests)||!restored.contests.length)return null;
  const stamp=catalogStamp(catalog);
  const saved=String(restored.catalog_stamp||"");
  if(stamp&&saved&&saved!==stamp){
    try{localStorage.removeItem(LS_PROJECT)}catch(_){}
    return null;
  }
  return restored;
}
function applyProjectObject(restored, opts){
  opts=opts||{};
  if(!restored||!Array.isArray(restored.contests)||!restored.contests.length)throw new Error("В файле нет contests[]");
  projectBlock=String(restored.block||BLOCK).toUpperCase()||BLOCK;
  contests=restored.contests.map((c,i)=>{
    const data=c.data||emptyContestData();
    pruneImportedEmptyRows(data);
    ensureJsonStructures(data);
    expandContestPrefixedCodes(data);
    syncBadgeSlots(data,false);
    seedLinked(data);
    sortScheduleRows(data.schedule);
    const item={id:c.id||("c"+Date.now()+"_"+i),name:c.name||"",stands:c.stands,data,baseline:null,userEdited:false};
    migrateContestStands(item, restored);
    markBaseline(item);
    return item;
  });
  activeContest=Math.min(Math.max(0,Number(restored.activeContest||0)),contests.length-1);
  archiveEntries=readArchiveFromProject(restored);
  activeArchiveId=null;
  activeArchiveSection="";
  contestSelectMode=false;
  selectedContestIds=new Set();
  activeSection="CONTEST";activeBadge=0;activeSchedule=0;activeIndicator=0;activeGroup=0;activeLink=0;activePairFocus=null;
  contestSelectMode=false;
  selectedContestIds=new Set();
  sessionReady=true;
  setGated(false);
  persistLocal();
  render();
  updatePageIntro(catalogSource);
  if(opts.toast)toast(opts.toast);
}
function restoreDraftIfAny(){
  const restored=peekDraft();
  if(!restored)return false;
  try{applyProjectObject(restored,{toast:"Черновик из браузера"});return true}catch(_){return false}
}
function startFresh(opts){
  opts=opts||{};
  contests=[];
  archiveEntries=[];
  activeArchiveId=null;
  contestSelectMode=false;
  selectedContestIds=new Set();
  const data=emptyContestData();
  const c={id:"c"+Date.now(),name:"",stands:[DEFAULT_STAND],data,baseline:null,userEdited:false};
  markBaseline(c);
  contests.push(c);
  activeContest=0;activeSection="CONTEST";activeBadge=0;activeSchedule=0;activeIndicator=0;activeGroup=0;activeLink=0;activePairFocus=null;
  sessionReady=true;
  setGated(false);
  persistLocal();
  render();
  updatePageIntro(catalogSource);
  toast(opts.toast||"Новый конкурс · значения по умолчанию из каталога");
}
function addContest(){
  if(!sessionReady){toast("Сначала выберите: открыть JSON или создать новый");return}
  const data=emptyContestData();
  const c={id:"c"+Date.now(),name:"",stands:[DEFAULT_STAND],data,baseline:null,userEdited:false};
  markBaseline(c);
  contests.push(c);
  activeContest=contests.length-1;activeSection="CONTEST";activeBadge=0;activeSchedule=0;activeIndicator=0;activeGroup=0;activeLink=0;activePairFocus=null;
  persistLocal();render();toast("Добавлен конкурс");
}
function resetToCatalogDefaults(){
  if(!confirm("Сбросить все конкурсы и заново взять значения по умолчанию из текущего каталога?\nОписания и варианты всегда из каталога; сброс касается заполненных значений."))return;
  try{localStorage.removeItem(LS_PROJECT)}catch(_){}
  startFresh({toast:"Сброшено к дефолтам каталога"});
}
function importProjectText(text){
  const data=JSON.parse(text);
  const nArch=readArchiveFromProject(data).length;
  const msg="Загружен снимок JSON ("+data.contests.length+" конкурс."+(nArch?", архив: "+nArch:"")+")";
  applyProjectObject(data,{toast:msg});
}
function onImportProjectFile(file){
  if(!file)return;
  const reader=new FileReader();
  reader.onload=()=>{
    try{importProjectText(String(reader.result||""))}
    catch(err){toast("Не удалось открыть JSON: "+(err&&err.message?err.message:err))}
  };
  reader.onerror=()=>toast("Ошибка чтения файла");
  reader.readAsText(file,"utf-8");
}
function setGated(on){
  document.querySelector(".app")?.classList.toggle("is-gated",!!on);
}
function showStartGate(){
  sessionReady=false;
  contests=[];
  archiveEntries=[];
  activeArchiveId=null;
  activeArchiveSection="";
  setGated(true);
  const draft=peekDraft();
  const nDraft=draft?draft.contests.length:0;
  const nArchDraft=draft?readArchiveFromProject(draft).length:0;
  $("workspace").innerHTML=`
    <section class="start-gate" id="start-gate">
      <h2 class="start-gate__title">С чего начнём?</h2>
      <p class="start-gate__text">
        Можно открыть ранее сохранённые настройки конкурсов (снимок JSON)
        или создать новый конкурс с полями и значениями по умолчанию из каталога.
      </p>
      <div class="start-gate__actions">
        <label class="btn btn-primary file-pick" id="gate-import-label" data-tip="Файл spod_fill_….json, сохранённый кнопкой «Сохранить снимок JSON»">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 21V9"/><path d="M7 14l5-5 5 5"/><path d="M5 3h14"/></svg>
          <span>Открыть сохранённые настройки (JSON)</span>
          <input type="file" id="gate-import-project" class="file-pick__input" accept=".json,application/json,text/json,text/plain" />
        </label>
        <button type="button" class="btn btn-sidebar" id="gate-fresh" data-tip="Один пустой конкурс с дефолтами каталога и шагами заполнения">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 5v14"/><path d="M5 12h14"/></svg>
          Создать новый конкурс с нуля
        </button>
        ${draft?`<button type="button" class="btn btn-sidebar" id="gate-draft" data-tip="Продолжить черновик: ${nDraft} конкурс.${nArchDraft?", архив: "+nArchDraft:""}">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M3 12a9 9 0 1 0 3-6.7"/><path d="M3 4v5h5"/></svg>
          Продолжить черновик (${nDraft}${nArchDraft?" · арх. "+nArchDraft:""})
        </button>`:""}
      </div>
      <p class="start-gate__hint">После старта слева появятся шаги заполнения. Снимок можно снова открыть в блоке «Проект».</p>
    </section>`;
  $("gate-fresh")?.addEventListener("click",()=>startFresh());
  $("gate-draft")?.addEventListener("click",()=>{if(!restoreDraftIfAny())toast("Черновик недоступен")});
  $("gate-import-project")?.addEventListener("change",e=>{
    const f=e.target.files&&e.target.files[0];
    onImportProjectFile(f);
    e.target.value="";
  });
  renderContestTabs();
  renderNav();
}
function updatePageIntro(source){
  const stamp=catalogStamp(catalog);
  const short=stamp?stamp.replace("T"," ").replace(/\.\d+Z$/," UTC"):"—";
  const text=sessionReady
    ?`Каталог: ${source||"embedded"} · ${short} · дефолты и описания из каталога`
    :`Каталог: ${source||"embedded"} · ${short} · выберите: открыть JSON или создать новый конкурс`;
  const intro=$("page-intro");
  if(intro){
    intro.textContent=text;
    intro.title=text;
  }
  const meta=document.querySelector(".foot-block--meta");
  if(meta) meta.title="Заполнение параметров SPOD · "+text;
}
async function boot(){
  const loaded=await loadCatalogPreferFile();
  catalog=loaded.data;
  catalogSource=loaded.source;
  ensureRewardTypeFilterButtons();
  syncContestKindFilterButtons();
  if(!catalog||!Array.isArray(catalog.sections)){
    $("workspace").innerHTML=`<section class="panel"><h2>Нет каталога</h2><p class="intro">Нужен <code>catalog.json</code> рядом со страницей или блок EMBEDDED_CATALOG.</p></section>`;
    return;
  }
  showStartGate();
  updatePageIntro(catalogSource);
  toast(loaded.source==="catalog.json"?"Каталог готов · выберите старт":"Встроенный каталог · выберите старт");
}

function initTips(){const tip=$("glassTip");document.addEventListener("mousemove",e=>{const node=e.target.closest("[data-tip]");if(!node){tip.classList.remove("show");return}const text=node.getAttribute("data-tip")||"";if(!text.trim()){tip.classList.remove("show");return}tip.textContent=text;tip.classList.add("show");let left=e.clientX+14,top=e.clientY+14;const r=tip.getBoundingClientRect();if(left+r.width>innerWidth-8)left=innerWidth-r.width-8;if(top+r.height>innerHeight-8)top=e.clientY-r.height-8;tip.style.left=Math.max(8,left)+"px";tip.style.top=Math.max(8,top)+"px"},true)}
function wireOutsideDate(){
  document.addEventListener("pointerdown",e=>{const open=document.querySelector(".date-pop.is-open");if(!open)return;if(e.target.closest(".date-pop")||e.target.closest(".default-date"))return;closeAllDatePops()},true);
  document.addEventListener("keydown",e=>{
    if(e.key==="Escape"){
      closeAllDatePops();
      return;
    }
    if((e.key==="b"||e.key==="B")&&(e.metaKey||e.ctrlKey)&&!e.altKey){
      const t=e.target;
      if(t&&(t.tagName==="INPUT"||t.tagName==="TEXTAREA"||t.tagName==="SELECT"||t.isContentEditable))return;
      e.preventDefault();
      setSidebarOpen(!sidebarOpen);
    }
  });
}

