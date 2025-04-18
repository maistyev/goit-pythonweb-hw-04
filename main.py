#!/usr/bin/env python3
import asyncio
import aiofiles
import argparse
import logging
import os
from pathlib import Path
import shutil
from typing import List, Set

# Налаштування логування
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def read_folder(source_dir: Path) -> List[Path]:
    """
    Асинхронно рекурсивно читає всі файли у вихідній папці та її підпапках.
    
    Args:
        source_dir: Шлях до вихідної папки.
    
    Returns:
        Список шляхів до всіх файлів.
    """
    all_files = []
    
    try:
        for entry in os.scandir(source_dir):
            path = Path(entry.path)
            if entry.is_file():
                all_files.append(path)
            elif entry.is_dir():
                subfolder_files = await read_folder(path)
                all_files.extend(subfolder_files)
    except Exception as e:
        logger.error(f"Помилка при читанні папки {source_dir}: {e}")
    
    return all_files

async def copy_file(file_path: Path, target_dir: Path) -> None:
    """
    Асинхронно копіює файл у відповідну підпапку в цільовій директорії.
    
    Args:
        file_path: Шлях до файлу, який потрібно скопіювати.
        target_dir: Шлях до цільової директорії.
    """
    try:
        # Отримуємо розширення файлу (без крапки)
        file_ext = file_path.suffix.lstrip('.')
        
        # Якщо файл не має розширення, використовуємо "no_extension"
        if not file_ext:
            file_ext = "no_extension"
        
        # Створюємо папку для розширення, якщо вона не існує
        ext_dir = target_dir / file_ext
        os.makedirs(ext_dir, exist_ok=True)
        
        # Шлях до цільового файлу
        target_file = ext_dir / file_path.name
        
        # Використовуємо aiofiles для асинхронного копіювання
        async with aiofiles.open(file_path, 'rb') as source_file:
            content = await source_file.read()
            
            async with aiofiles.open(target_file, 'wb') as dest_file:
                await dest_file.write(content)
                
        logger.info(f"Файл {file_path.name} успішно скопійовано в {ext_dir}")
    except Exception as e:
        logger.error(f"Помилка при копіюванні файлу {file_path}: {e}")

async def process_files(source_dir: Path, target_dir: Path) -> None:
    """
    Обробляє всі файли з вихідної папки і розміщує їх у цільовій папці.
    
    Args:
        source_dir: Шлях до вихідної папки.
        target_dir: Шлях до цільової папки.
    """
    # Отримуємо список всіх файлів
    files = await read_folder(source_dir)
    logger.info(f"Знайдено {len(files)} файлів для обробки")
    
    # Створюємо та запускаємо завдання для кожного файлу
    tasks = [copy_file(file, target_dir) for file in files]
    await asyncio.gather(*tasks)
    
    logger.info("Всі файли успішно оброблено")

async def main():
    # Створюємо парсер аргументів
    parser = argparse.ArgumentParser(
        description='Асинхронно сортує файли за розширеннями з вихідної в цільову папку'
    )
    
    # Додаємо аргументи
    parser.add_argument(
        '-s', '--source',
        type=str,
        required=True,
        help='Шлях до вихідної папки з файлами'
    )
    
    parser.add_argument(
        '-t', '--target',
        type=str,
        required=True,
        help='Шлях до цільової папки, де будуть створені підпапки за розширеннями'
    )
    
    # Парсимо аргументи
    args = parser.parse_args()
    
    # Перетворюємо шляхи у Path об'єкти
    source_dir = Path(args.source)
    target_dir = Path(args.target)
    
    # Перевіряємо, чи існує вихідна папка
    if not source_dir.exists() or not source_dir.is_dir():
        logger.error(f"Вихідна папка {source_dir} не існує або не є папкою")
        return
    
    # Створюємо цільову папку, якщо вона не існує
    os.makedirs(target_dir, exist_ok=True)
    
    # Запускаємо обробку файлів
    await process_files(source_dir, target_dir)

if __name__ == "__main__":
    # Запускаємо асинхронний головний блок
    asyncio.run(main())