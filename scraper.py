"""
A web scraper for grabbing clues from the j-archive website.

@author: Riley Smith
Created: 11/26/2023
"""
from datetime import datetime
import json
from pathlib import Path
import time

from bs4 import BeautifulSoup
import pandas as pd
import requests
from tqdm import tqdm

BASE_URL = "https://j-archive.com/showgame.php?game_id={}"

def filter_clue_list(clue_list):
    """
    Remove clues with 'r' in their ID (not sure why these are in the
    J archive website, but they need to go).
    """
    filtered = [item for item in clue_list if not item['id'][-2:] == '_r']
    return filtered

def format_clue(table_element, categories):
    """
    This will take a clue html tag and extract the text, point value, and
    category.
    """
    # Get relevant sub-elements
    sub_elements = table_element.find_all('td', {'class': 'clue_text'})
    try:
        clue = sub_elements[0]
        response = sub_elements[1]
    except:
        breakpoint()
    answer = response.find_all('em', {'class': 'correct_response'})[0].text
    # Get category and point value
    clue_id = clue.get('id')
    if clue_id == 'clue_FJ':
        category = categories[-1]
        clue_value = 'FINAL JEOPARDY'
    else:
        dj = 'DJ' in clue_id
        id_parts = clue_id.split('_')
        cat_id = int(id_parts[2].strip())
        clue_value = 200 * int(id_parts[3])
        if dj:
            clue_value *= 2
            cat_id += 6
        category = categories[cat_id]

    # Return it as a dict
    out_dict = {
        'clue': clue.text, 
        'answer': answer,
        'category': category, 
        'value': clue_value
    }
    return out_dict

def scrape_clues(soup):
    """
    For a given game, return a list of all the clues.
    """
    # Fetch categories
    categories = soup.find_all('td', {'class': 'category_name'})
    categories = [cat.text for cat in categories]
    # Fetch all clues
    clues = soup.find_all('td', {'class': 'clue_text'})
    # Get parent elements
    table_elements = [clue.parent for clue in clues if not clue['id'][-2:] == '_r']
    # Format each one
    clues = [format_clue(element, categories) for element in table_elements]
    clues = [clue for clue in clues if clue is not None]
    return clues

    # Filter it
    filtered_clues = [clue for clue in clues if not clue['id'][-2:] == '_r']
    return [clue.text for clue in filtered_clues]

def scrape_episode(game_id):
    """
    Scrape clues, answers, and date for a given episode.
    """
    # Get the URL for this game
    url = BASE_URL.format(str(game_id))
    # Scrape it
    page = requests.get(url, timeout=10)
    soup = BeautifulSoup(page.content, 'html.parser')
    # Get the date for this game
    title = soup.find_all('div', {'id': 'game_title'})[0]
    date_str = title.find_all('h1')[0].text.split(' - ')[1]
    date_obj = datetime.strptime(date_str, '%A, %B %d, %Y')
    # Now get clues and responses
    clues = scrape_clues(soup)
    # And return it as a dict
    return {
        'date': date_obj,
        'id': game_id,
        'clues': clues
    }

def to_csv(game_info, csvfile):
    """Write the game info to a CSV file"""
    mode = 'a' if Path(csvfile).exists() else 'w'
    header = not Path(csvfile).exists()
    date_str = game_info['date'].strftime('%m/%d/%Y')
    data = game_info['clues']
    df = pd.DataFrame(data)
    df['date'] = date_str
    df['game_id'] = game_info['id']
    df.to_csv(csvfile, index=False, mode=mode, header=header)

def scrape_ids(season_number):
    season_url = f'https://j-archive.com/showseason.php?season={season_number}'
    page = requests.get(season_url)
    soup = BeautifulSoup(page.content, 'html.parser')
    links = soup.find_all('a')
    links = [l for l in links if 'aired' in l.text]
    game_ids = [int(l.get('href').split('game_id=')[1]) for l in links]
    return game_ids[::-1]

def order_ids():
    ids = []
    for season_number in tqdm(range(40)):
        season_ids = scrape_ids(season_number)
        ids += season_ids
    with open('id_order.json', 'w+') as json_file:
        json.dump(ids, json_file)

def load_id_order():
    with open('id_order.json', 'r') as json_file:
        return json.load(json_file)

def scrape_all(csvfile, crawl_delay=0, resume=False):
    """
    Scrape a bunch of game data.
    """
    if Path(csvfile).exists() and (not resume):
        raise ValueError('csvfile already exists')
    elif resume:
        # Read which IDs have been scraped already
        data = pd.read_csv(csvfile)
        done_ids = data['game_id'].unique().tolist()
    
    # Scrape each game
    ids = load_id_order()[::-1]
    error_filepath = 'errors.txt'
    if Path(error_filepath).exists():
        Path(error_filepath).unlink()
    Path(error_filepath).touch()
    for game_id in tqdm(ids):
        # Skip if this game has already been scraped
        if resume and game_id in done_ids:
            continue
        try:
            game_info = scrape_episode(game_id)
        except Exception as e:
            with open(error_filepath, 'a') as fp:
                fp.write(str(game_id))
                fp.write(str(repr(e)))
                fp.write('\n\n')
            continue
        to_csv(game_info, csvfile)
        if crawl_delay > 0:
            time.sleep(crawl_delay)

if __name__ == '__main__':
    # game_id = 7091
    # game_info = scrape_episode(game_id)
    scrape_all('jeopardy_data.csv', 2, resume=True)