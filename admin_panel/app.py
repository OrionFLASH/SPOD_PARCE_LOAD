"""
Главное приложение Flask для админ-панели
"""
from flask import Flask, render_template, jsonify, request
from urllib.parse import unquote
import json
import logging


def format_value_for_log(value, max_length=100):
    """Форматирует значение для лога: короткие полностью, длинные обрезаются"""
    if value is None:
        return "None"
    
    value_str = str(value)
    
    if len(value_str) <= max_length:
        return value_str
    else:
        return value_str[:max_length] + f"... (длина: {len(value_str)})"

def format_record_for_log(record, max_field_length=100):
    """Форматирует запись для лога"""
    if not record:
        return "{}"
    
    formatted = {}
    for key, value in record.items():
        formatted[key] = format_value_for_log(value, max_field_length)
    
    return formatted

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.file_manager import FileManager
from utils.data_manager import DataManager
from utils.json_editor import JSONEditor
import config

app = Flask(__name__)

# Настройка логирования

def get_log_filename(level='DEBUG', module='admin_panel'):
    """Генерирует имя файла лога в формате LOGS_уровень_модуль_YYYYMMDD_HHMM.log"""
    import os
    from datetime import datetime
    
    # Каталог LOGS в корне проекта (на уровень выше admin_panel)
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    logs_dir = os.path.join(base_dir, 'LOGS')
    os.makedirs(logs_dir, exist_ok=True)
    
    # Формируем имя файла
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    filename = f"LOGS_{level}_{module}_{timestamp}.log"
    log_path = os.path.join(logs_dir, filename)
    
    return log_path


logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s | %(levelname)s | %(name)s | %(funcName)s | %(message)s",
    handlers=[
        logging.FileHandler(get_log_filename("DEBUG", "admin_panel"), encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

app.config["SECRET_KEY"] = "spod-admin-panel-secret-key"

file_manager = FileManager()
data_manager = DataManager()
json_editor = JSONEditor()

@app.route("/")
def index():
    """Главная страница"""
    # Проверяем или создаем сессию редактирования
    edit_session = file_manager.get_edit_session()
    if not edit_session:
        edit_session = file_manager.create_edit_session()
    
    return render_template("index.html", edit_session=os.path.basename(edit_session))

@app.route("/api/files")
def get_files():
    """Список файлов"""
    return jsonify({
        "files": list(config.FILE_NAMES.keys()),
        "file_names": config.FILE_NAMES
    })


@app.route("/api/sessions")
def get_sessions():
    """Получение списка всех сессий редактирования"""
    import os
    # Путь к корню проекта
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    edit_dir = os.path.join(base_dir, config.DIR_EDIT)
    if not os.path.exists(edit_dir):
        return jsonify({"sessions": []})
    
    sessions = [d for d in os.listdir(edit_dir) 
                if os.path.isdir(os.path.join(edit_dir, d))]
    sessions.sort(reverse=True)  # Новые сверху
    return jsonify({"sessions": sessions})

@app.route("/api/session/current")
def get_current_session():
    """Получение текущей активной сессии"""
    edit_session = file_manager.get_edit_session()
    if edit_session:
        return jsonify({"session": os.path.basename(edit_session)})
    return jsonify({"session": None})

@app.route("/api/session/<session_name>", methods=["POST"])
def set_session(session_name):
    """Установка активной сессии"""
    import os
    # Путь к корню проекта (на уровень выше admin_panel)
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    edit_dir = os.path.join(base_dir, config.DIR_EDIT)
    session_path = os.path.join(edit_dir, session_name)
    
    if not os.path.exists(session_path):
        return jsonify({"error": f"Сессия не найдена: {session_path}"}), 404
    
    file_manager.current_edit_dir = session_path
        
    logger.info(f"Переключение на сессию: {session_name}")
    logger.debug(f"Переключение на сессию: {session_name}, путь: {session_path}")
    return jsonify({"message": "Сессия установлена", "session": session_name})

@app.route("/api/session/new", methods=["POST"])
def create_new_session():
    """Создание новой сессии редактирования"""
    logger.info("Запрос на создание новой сессии")
    logger.debug("Запрос на создание новой сессии от пользователя")
    try:
        session_path = file_manager.create_edit_session()
        session_name = os.path.basename(session_path)
        logger.info(f"Создана новая сессия: {session_name}")
        logger.debug(f"Создана новая сессия: {session_name}, путь: {session_path}")
        return jsonify({"message": "Новая сессия создана", "session": session_name})
    except Exception as e:
        logger.exception(f"Ошибка создания сессии: {e}")
        return jsonify({"error": str(e)}), 500





@app.route('/api/session/<session_name>/info', methods=['GET'])
def get_session_info(session_name):
    """Получение информации о сессии (список файлов)"""
    try:
        import urllib.parse
        session_name = urllib.parse.unquote(session_name)
        
        session_path = os.path.join(file_manager.edit_dir, session_name)
        
        if not os.path.exists(session_path):
            return jsonify({'error': f'Сессия {session_name} не найдена'}), 404
        
        # Получаем список файлов
        files = []
        if os.path.isdir(session_path):
            files = [f for f in os.listdir(session_path) if f.endswith('.csv')]
            files.sort()
        
        return jsonify({
            'session': session_name,
            'files': files,
            'files_count': len(files)
        })
    except Exception as e:
        logger.error(f"Ошибка получения информации о сессии {session_name}: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/session/<session_name>', methods=['DELETE'])
def delete_session(session_name):
    """Удаление сессии редактирования"""
    try:
        import urllib.parse
        session_name = urllib.parse.unquote(session_name)
        
        # Получаем список файлов в сессии перед удалением
        session_path = os.path.join(file_manager.edit_dir, session_name)
        files_list = []
        
        if os.path.exists(session_path):
            files = os.listdir(session_path)
            files_list = [f for f in files if f.endswith('.csv')]
        
        logger.info(f"Удаление сессии: {session_name}")
        logger.debug(f"Удаление сессии: {session_name}, файлов в сессии: {len(files_list)}")
        
        # Удаляем сессию
        result = file_manager.delete_session(session_name)
        
        if result.get('error'):
            return jsonify(result), 400
        
        logger.debug(f"Сессия {session_name} удалена, удалено файлов: {len(files_list)}")
        return jsonify({
            'success': True,
            'session': session_name,
            'deleted_files': files_list,
            'message': f'Сессия {session_name} удалена'
        })
    except Exception as e:
        logger.error(f"Ошибка удаления сессии {session_name}: {e}")
        return jsonify({'error': str(e)}), 500
@app.route("/api/files/<file_key>/records")
def get_records(file_key):
    file_key = unquote(file_key)  # Декодируем URL
    """Получение записей с пагинацией"""
    try:
        logger.debug(f"get_records: file_key={file_key}, args={dict(request.args)}")
        page = int(request.args.get("page", 1))
        per_page = int(request.args.get("per_page", 50))
        search = request.args.get("search", "")
        sort_by = request.args.get("sort_by")
        sort_order = request.args.get("sort_order", "asc")
        
        logger.debug(f"Параметры: page={page}, per_page={per_page}, search={search}, sort_by={sort_by}")
        result = data_manager.get_records(file_key, page, per_page, search, sort_by, sort_order)
        logger.debug(f"Результат: {len(result.get('records', []))} записей, всего {result.get('total', 0)}")
        return jsonify(result)
    except Exception as e:
        logger.exception(f"Ошибка в get_records для {file_key}: {e}")
        return jsonify({"error": str(e)}), 400

@app.route("/api/files/<file_key>/records/<int:record_id>")
def get_record(file_key, record_id):
    """Получение одной записи"""
    try:
        logger.debug(f"get_record: file_key={file_key}, record_id={record_id}")
        record = data_manager.get_record(file_key, record_id)
        if record is None:
            logger.warning(f"Запись {record_id} не найдена в {file_key}")
            return jsonify({"error": "Запись не найдена"}), 404
        logger.debug(f"Запись получена: {len(record)} полей")
        return jsonify(record)
    except Exception as e:
        logger.exception(f"Ошибка в get_record для {file_key}, record_id={record_id}: {e}")
        return jsonify({"error": str(e)}), 400


@app.route("/api/files/<file_key>/json-field/<field_name>/structure")
def get_json_field_structure(file_key, field_name):
    """Получение структуры JSON поля для автодополнения"""
    try:
        file_key = unquote(file_key)
        field_name = unquote(field_name)
        logger.debug(f"get_json_field_structure: file_key={file_key}, field_name={field_name}")
        
        # Получаем все записи с этим полем
        df = data_manager.file_manager.read_csv(file_key)
        
        if field_name not in df.columns:
            return jsonify({"error": f"Поле {field_name} не найдено"}), 404
        
        # Анализируем структуру JSON
        samples = df[field_name].dropna().head(100)
        structures = []
        field_types = {}
        
        for sample in samples:
            if isinstance(sample, str):
                try:
                    data = json.loads(sample)
                    if isinstance(data, dict):
                        structures.append(data)
                        for key, value in data.items():
                            if key not in field_types:
                                field_types[key] = set()
                            field_types[key].add(type(value).__name__)
                except:
                    pass
        
        # Определяем общие поля и их типы
        common_fields = {}
        if structures:
            all_keys = set()
            for s in structures:
                all_keys.update(s.keys())
            
            for key in all_keys:
                values = [s.get(key) for s in structures if key in s]
                if values:
                    types = set(type(v).__name__ for v in values)
                    common_fields[key] = {
                        "type": list(types),
                        "example": values[0] if values else None
                    }
        
        # Проверяем зависимости
        dependencies = {}
        if hasattr(config, 'JSON_FIELD_DEPENDENCIES'):
            deps = config.JSON_FIELD_DEPENDENCIES.get(file_key, {}).get(field_name, {})
            if deps:
                depends_on = deps.get('depends_on')
                if depends_on and depends_on in df.columns:
                    # Группируем по значению зависимости
                    for dep_value in df[depends_on].unique()[:10]:
                        subset = df[df[depends_on] == dep_value][field_name].dropna()
                        if len(subset) > 0:
                            sample = subset.iloc[0]
                            if isinstance(sample, str):
                                try:
                                    data = json.loads(sample)
                                    if isinstance(data, dict):
                                        dependencies[dep_value] = list(data.keys())
                                except:
                                    pass
        
        return jsonify({
            "field_name": field_name,
            "common_fields": common_fields,
            "dependencies": dependencies,
            "sample_count": len(structures)
        })
    except Exception as e:
        logger.exception(f"Ошибка получения структуры JSON поля: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/files/<file_key>/field/<field_name>/values")
def get_field_values(file_key, field_name):
    """Получение списка возможных значений для поля"""
    try:
        file_key = unquote(file_key)
        field_name = unquote(field_name)
        logger.debug(f"get_field_values: file_key={file_key}, field_name={field_name}")
        
        # Проверяем MULTI_VALUE_FIELDS
        if hasattr(config, 'MULTI_VALUE_FIELDS'):
            multi_fields = config.MULTI_VALUE_FIELDS.get(file_key, {}).get(field_name)
            if multi_fields:
                # Получаем значения из исходного файла
                source_file = multi_fields.get('source_file')
                source_field = multi_fields.get('source_field')
                
                if source_file and source_field:
                    df = data_manager.file_manager.read_csv(source_file)
                    if source_field in df.columns:
                        values = sorted(df[source_field].dropna().unique().tolist())
                        logger.debug(f"Найдено {len(values)} уникальных значений")
                        return jsonify({
                            "field_name": field_name,
                            "values": values,
                            "source": f"{source_file}.{source_field}",
                            "type": "multi_value"
                        })
        
        # Если не MULTI_VALUE_FIELD, получаем уникальные значения из текущего файла
        df = data_manager.file_manager.read_csv(file_key)
        if field_name in df.columns:
            values = sorted(df[field_name].dropna().unique().tolist())
            logger.debug(f"Найдено {len(values)} уникальных значений из текущего файла")
            return jsonify({
                "field_name": field_name,
                "values": values[:100],  # Ограничиваем 100 значениями
                "type": "unique_values"
            })
        else:
            return jsonify({"error": f"Поле {field_name} не найдено"}), 404
            
    except Exception as e:
        logger.exception(f"Ошибка получения значений поля: {e}")
        return jsonify({"error": str(e)}), 500



if __name__ == "__main__":
    logger.info("Запуск сервера админ-панели...")
    logger.info("Сервер готов к работе")
    app.run(debug=True, host='0.0.0.0', port=5001)
