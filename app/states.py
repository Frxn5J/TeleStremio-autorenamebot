from aiogram.fsm.state import State, StatesGroup


class MediaForm(StatesGroup):
    confirming_llm = State()
    confirming_tmdb = State()
    choosing_type = State()
    movie_name = State()
    movie_year = State()
    movie_quality = State()
    movie_optional = State()
    series_name = State()
    series_season = State()
    series_episode = State()
    series_quality = State()
    series_optional = State()
    confirming = State()
