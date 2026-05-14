from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder


def llm_confirm_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Enviar al canal", callback_data="llm:send"),
                InlineKeyboardButton(text="Editar manual", callback_data="llm:edit"),
            ]
        ]
    )


def tmdb_confirm_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Sí, es correcto", callback_data="tmdb:yes"),
                InlineKeyboardButton(text="No, continuar manual", callback_data="tmdb:no"),
            ]
        ]
    )


def type_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="Película", callback_data="type:movie")
    builder.button(text="Serie", callback_data="type:series")
    builder.adjust(2)
    return builder.as_markup()


def confirm_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Enviar al canal", callback_data="confirm:send"),
                InlineKeyboardButton(text="Cancelar", callback_data="confirm:cancel"),
            ]
        ]
    )


def detected_quality_keyboard(quality: str | None, prefix: str) -> InlineKeyboardMarkup | None:
    if not quality:
        return None
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=f"Usar {quality}", callback_data=f"{prefix}:detected_quality")]
        ]
    )


def optional_keyboard(prefix: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Sin extras", callback_data=f"{prefix}:none")]
        ]
    )
