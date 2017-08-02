#!/usr/bin/env python
# -*- coding: utf-8 -*-

# -----------------
# Реализуйте функцию best_hand, которая принимает на вход
# покерную "руку" (hand) из 7ми карт и возвращает лучшую
# (относительно значения, возвращаемого hand_rank)
# "руку" из 5ти карт. У каждой карты есть масть(suit) и
# ранг(rank)
# Масти: трефы(clubs, C), пики(spades, S), червы(hearts, H), бубны(diamonds, D)
# Ранги: 2, 3, 4, 5, 6, 7, 8, 9, 10 (ten, T), валет (jack, J), дама (queen, Q), король (king, K), туз (ace, A)
# Например: AS - туз пик (ace of spades), TH - дестяка черв (ten of hearts), 3C - тройка треф (three of clubs)

# Задание со *
# Реализуйте функцию best_wild_hand, которая принимает на вход
# покерную "руку" (hand) из 7ми карт и возвращает лучшую
# (относительно значения, возвращаемого hand_rank)
# "руку" из 5ти карт. Кроме прочего в данном варианте "рука"
# может включать джокера. Джокеры могут заменить карту любой
# масти и ранга того же цвета, в колоде два джокерва.
# Черный джокер '?B' может быть использован в качестве треф
# или пик любого ранга, красный джокер '?R' - в качестве черв и бубен
# любого ранга.

# Одна функция уже реализована, сигнатуры и описания других даны.
# Вам наверняка пригодится itertoolsю
# Можно свободно определять свои функции и т.п.
# -----------------

import itertools


def hand_rank(hand):
    """Возвращает значение определяющее ранг 'руки'"""
    ranks = card_ranks(hand)
    if straight(ranks) and flush(hand):
        return (8, max(ranks))
    elif kind(4, ranks):
        return (7, kind(4, ranks), kind(1, ranks))
    elif kind(3, ranks) and kind(2, ranks):
        return (6, kind(3, ranks), kind(2, ranks))
    elif flush(hand):
        return (5, ranks)
    elif straight(ranks):
        return (4, max(ranks))
    elif kind(3, ranks):
        return (3, kind(3, ranks), ranks)
    elif two_pair(ranks):
        return (2, two_pair(ranks), ranks)
    elif kind(2, ranks):
        return (1, kind(2, ranks), ranks)
    else:
        return (0, ranks)


def card_ranks(hand):
    """Возвращает список рангов (его числовой эквивалент),
    отсортированный от большего к меньшему"""
    def card_to_rank(card):
        rank = card[0]
        value = {'T': 10, 'J': 11, 'Q': 12, 'K': 13}.get(rank)
        return value if value else int(rank)
    ranks = [card_to_rank(card) for card in hand]
    return sorted(ranks, reverse=True)


def flush(hand):
    """Возвращает True, если все карты одной масти"""
    suits = (card[1] for card in hand)
    return len(set(suits)) == 1


def straight(ranks):
    """Возвращает True, если отсортированные ранги формируют последовательность 5ти,
    где у 5ти карт ранги идут по порядку (стрит)"""
    prev_rank = ranks[0]
    for rank in ranks[1:]:
        if rank != prev_rank - 1:
            return False
        prev_rank = rank
    return True


def kind(n, ranks):
    """Возвращает первый ранг, который n раз встречается в данной руке.
    Возвращает None, если ничего не найдено"""
    rank_to_count = {r: 0 for r in ranks}
    for rank in ranks:
        rank_to_count[rank] += 1
    for rank in ranks:
        count = rank_to_count[rank]
        if count == n:
            return rank


def two_pair(ranks):
    """Если есть две пары, то возврщает два соответствующих ранга,
    иначе возвращает None"""
    rank_to_count = {r: 0 for r in ranks}
    for rank in ranks:
        rank_to_count[rank] += 1
    pairs = []
    for rank in sorted(set(ranks), reverse=True):
        count = rank_to_count[rank]
        if count >= 2:
            pairs.append(rank)
    if len(pairs) == 2:
        return pairs


def best_hand(hand):
    """Из "руки" в 7 карт возвращает лучшую "руку" в 5 карт """
    hands = list(itertools.combinations(hand, 5))
    ranks = [hand_rank(hand) for hand in hands]
    index = ranks.index(max(ranks))
    return hands[index]


def wild_hand_to_hands(hand):
    black_cards = [rank + suit for rank, suit in itertools.product(map(str, range(2, 10)) + ['T', 'J', 'Q', 'K'], ['C', 'S'])]
    red_cards = [rank + suit for rank, suit in itertools.product(map(str, range(2, 10)) + ['T', 'J', 'Q', 'K'], ['H', 'D'])]
    hands = []
    black_count = 0
    red_count = 0
    cards_wo_jokers = []
    for card in hand:
        if card == '?B':
            black_count += 1
        elif card == '?R':
            red_count += 1
        else:
            cards_wo_jokers.append(card)
    combinations = itertools.product(*([black_cards] * black_count + [red_cards] * red_count))
    for cards in combinations:
        hand = cards_wo_jokers + list(cards)
        hands.append(hand)
    return hands


def best_hand_and_rank(hand):
    """Из "руки" в 7 карт возвращает лучшую "руку" в 5 карт """
    hands = list(itertools.combinations(hand, 5))
    hand = max(hands, key=hand_rank)
    return hand, hand_rank(hand)


def best_wild_hand(hand):
    """best_hand но с джокерами"""
    hands = wild_hand_to_hands(hand)
    best_hands = []
    ranks = []
    for hand in hands:
        best_hand_, rank = best_hand_and_rank(hand)
        best_hands.append(best_hand_)
        ranks.append(ranks)
    index = ranks.index(max(ranks))
    return best_hands[index]


def test_best_hand():
    print "test_best_hand..."
    assert (sorted(best_hand("6C 7C 8C 9C TC 5C JS".split()))
            == ['6C', '7C', '8C', '9C', 'TC'])
    assert (sorted(best_hand("TD TC TH 7C 7D 8C 8S".split()))
            == ['8C', '8S', 'TC', 'TD', 'TH'])
    assert (sorted(best_hand("JD TC TH 7C 7D 7S 7H".split()))
            == ['7C', '7D', '7H', '7S', 'JD'])
    print 'OK'


def test_best_wild_hand():
    print "test_best_wild_hand..."
    assert (sorted(best_wild_hand("6C 7C 8C 9C TC 5C ?B".split()))
            == ['7C', '8C', '9C', 'JC', 'TC'])
    assert (sorted(best_wild_hand("TD TC 5H 5C 7C ?R ?B".split()))
            == ['7C', 'TC', 'TD', 'TH', 'TS'])
    assert (sorted(best_wild_hand("JD TC TH 7C 7D 7S 7H".split()))
            == ['7C', '7D', '7H', '7S', 'JD'])
    print 'OK'

if __name__ == '__main__':
    test_best_hand()
    best_wild_hand("6C 7C 8C ?R TC 5C ?B".split())
    # test_best_wild_hand()
