package ru.mfilatov.SmallTests.dpl;

/**
 * Значение для кэша глобальных индексов. Данные не хранит, но класс необходим, т.к. в кэш нельзя вставить null значение
 */
public final class GlobalIndexValue {
    /**
     * Т.к. нельзя вставить null значение в кэш, то создается объект заглушка
     */
    public static final GlobalIndexValue VALUE = new GlobalIndexValue();

    private GlobalIndexValue() {}
}
