### Categorias de Validações de Qualidade de Dados

#### 1. Crítico
Essas são validações que, se falharem, indicam problemas sérios nos dados que precisam ser corrigidos imediatamente. Falhas nesse nível podem interromper processos de negócio ou causar danos significativos.

**Exemplos:**
- **Validações de Schema**: A estrutura dos dados não corresponde ao esperado (colunas ausentes ou adicionais).
- **Consistência de Chaves Primárias**: Duplicação de chaves primárias que devem ser únicas.
- **Integridade Referencial**: Chaves estrangeiras que não correspondem a uma chave primária em outra tabela.

#### 2. Alto
Validações que são importantes, mas podem ser tratadas dentro de um prazo definido. Problemas aqui podem levar a resultados imprecisos ou atrasos.

**Exemplos:**
- **Dados Faltantes**: Campos obrigatórios ausentes em uma porcentagem significativa de registros.
- **Valores Inválidos**: Dados que não estão dentro dos valores esperados ou aceitáveis (ex.: valores negativos em colunas de quantidade).

#### 3. Médio
Validações que afetam a qualidade dos dados, mas não são urgentes. Falhas aqui podem ser corrigidas em ciclos regulares de manutenção.

**Exemplos:**
- **Padrões de Formatação**: Endereços de email, números de telefone, ou outros campos que não seguem o formato esperado.
- **Consistência de Dados**: Verificação de que valores correlacionados entre diferentes colunas estão consistentes (ex.: estado e cidade).

#### 4. Baixo
Validações que têm um impacto menor na qualidade geral dos dados e podem ser corrigidas como parte de um ciclo de melhoria contínua.

**Exemplos:**
- **Verificações de Duplicação**: Duplicação de registros não críticos.
- **Validação de Dados Opcionais**: Campos opcionais que contêm dados inesperados ou formatos incorretos.

### Tipos de Validações

Para o atributo "type", aqui estão algumas sugestões que podem ser usadas para categorizar as validações:

1. **schema**
   - Verificação da estrutura dos dados, incluindo a presença de colunas obrigatórias e a conformidade com o esquema esperado.

2. **uniqueness**
   - Garantia de que determinados campos, como chaves primárias, sejam únicos em todo o conjunto de dados.

3. **referential_integrity**
   - Validação da integridade referencial, assegurando que as chaves estrangeiras correspondam a chaves primárias em outras tabelas.

4. **null_check**
   - Verificação da presença de valores nulos em campos que não devem ser nulos.

5. **format**
   - Validação de que os dados seguem um formato específico, como endereços de email, números de telefone, datas, etc.

6. **range**
   - Garantia de que os valores numéricos ou de data estão dentro de um intervalo aceitável.

7. **consistency**
   - Verificação da consistência entre colunas correlacionadas (ex.: estado e cidade).

8. **completeness**
   - Avaliação da completude dos dados, assegurando que todos os campos obrigatórios estejam preenchidos.

9. **duplicate_check**
   - Identificação de registros duplicados que não deveriam existir.

10. **business_rule**
    - Validação de regras de negócio específicas que os dados devem atender.

11. **pattern**
    - Verificação de que os dados correspondem a um padrão regex específico.

12. **custom_sql**
    - Validações customizadas realizadas através de consultas SQL específicas.

13. **statistical**
    - Validações baseadas em estatísticas, como médias, medianas, desvios padrões, etc.


```json
{
  "validations": [
    {
      "type": "schema",
      "level": "Crítico",
      "description": "Ensures all mandatory fields are present."
    },
    {
      "type": "uniqueness",
      "level": "Crítico",
      "description": "Ensures primary keys are unique."
    },
    {
      "type": "format",
      "level": "Médio",
      "description": "Ensures email addresses follow a valid format."
    },
    {
      "type": "null_check",
      "level": "Alto",
      "description": "Ensures important fields do not contain null values."
    },
    {
      "type": "referential_integrity",
      "level": "Crítico",
      "description": "Ensures foreign keys match primary keys in related tables."
    }
  ]
}
```


