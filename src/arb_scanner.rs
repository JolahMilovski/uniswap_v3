// Функция вычисляет следующую цену sqrt из входного значения
pub fn get_next_sqrt_price_from_input(
    sqrt_price_x96: U256,    // Текущая цена sqrt в формате Q96.96
    liquidity: U256,         // Текущая ликвидность пула
    amount_in: U256,         // Входящее количество токенов
    zero_for_one: bool,      // Направление свопа (true если token0 -> token1)
) -> U256 {
    if zero_for_one {
        // Если своп token0 -> token1
        let numerator = liquidity << 96;    // Умножаем ликвидность на 2^96 для соответствия формату
        let product = amount_in.checked_mul(sqrt_price_x96).unwrap();    // Умножаем входящее количество на текущую цену sqrt
        let denominator = numerator.checked_add(product).unwrap();    // Добавляем произведение к числителю
        numerator.checked_mul(sqrt_price_x96).unwrap() / denominator    // Вычисляем новую цену sqrt
    } else {
        // Если своп token1 -> token0
        let product = amount_in.checked_mul(U256::from(1u128 << 96)).unwrap();    // Умножаем входящее количество на 2^96
        sqrt_price_x96
            .checked_add(product.checked_div(liquidity).unwrap())    // Добавляем к текущей цене sqrt частное от деления
            .unwrap()
    }
}

// Функция вычисляет следующую цену sqrt из выходного значения
pub fn get_next_sqrt_price_from_output(
    sqrt_price_x96: U256,    // Текущая цена sqrt в формате Q96.96
    liquidity: U256,         // Текущая ликвидность пула
    amount_out: U256,        // Исходящее количество токенов
    zero_for_one: bool,      // Направление свопа (true если token0 -> token1)
) -> U256 {
    if zero_for_one {
        // Если своп token0 -> token1
        let product = amount_out.checked_mul(U256::from(1u128 << 96)).unwrap();    // Умножаем исходящее количество на 2^96
        sqrt_price_x96
            .checked_add(product.checked_div(liquidity).unwrap())    // Добавляем к текущей цене sqrt частное от деления
            .unwrap()
    } else {
        // Если своп token1 -> token0
        let numerator = liquidity << 96;    // Умножаем ликвидность на 2^96 для соответствия формату
        let product = amount_out.checked_mul(sqrt_price_x96).unwrap();    // Умножаем исходящее количество на текущую цену sqrt
        let denominator = numerator.checked_sub(product).unwrap();    // Вычитаем произведение из числителя
        numerator.checked_mul(sqrt_price_x96).unwrap() / denominator    // Вычисляем новую цену sqrt
    }
}