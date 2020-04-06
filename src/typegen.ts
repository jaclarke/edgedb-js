#!/usr/bin/env node
import { readFile, writeFile } from 'fs'
import { promisify } from 'util'
import { parse as parsePath, format as formatPath } from 'path'

import * as yargs from 'yargs'
import * as glob from 'glob'
import * as chalk from 'chalk'

import { connect } from './index'
import { AwaitConnection } from './client'
import { KNOWN_TYPES } from './codecs/codecs'
import { ICodec, ScalarCodec } from './codecs/ifaces'
import { ObjectCodec } from './codecs/object'
import { EnumCodec } from './codecs/enum'
import { SetCodec } from './codecs/set'
import { TupleCodec, EmptyTupleCodec } from './codecs/tuple'
import { NamedTupleCodec } from './codecs/namedtuple'
import { ArrayCodec } from './codecs/array'

const readFileAsync = promisify(readFile)
const writeFileAsync = promisify(writeFile)

interface TypingInfo {
  type: string
  needsImport?: true
}

enum Cardinality {
  ZERO = 0x6e,
  ONE = 0x6f,
  MANY = 0x6d,
}

const ScalarTypings = new Map<string, TypingInfo>([
  ['std::uuid',           {type: 'UUID', needsImport: true}],
  ['std::str',            {type: 'string'}],
  ['std::bytes',          {type: 'Buffer'}],
  ['std::int16',          {type: 'number'}],
  ['std::int32',          {type: 'number'}],
  ['std::int64',          {type: 'number'}],
  ['std::float32',        {type: 'number'}],
  ['std::float64',        {type: 'number'}],
  ['std::bool',           {type: 'boolean'}],
  ['std::datetime',       {type: 'Date'}],
  ['std::local_datetime', {type: 'LocalDateTime', needsImport: true}],
  ['std::local_date',     {type: 'LocalDate', needsImport: true}],
  ['std::local_time',     {type: 'LocalTime', needsImport: true}],
  ['std::duration',       {type: 'Duration', needsImport: true}],
  ['std::json',           {type: 'string'}],
  ['std::bigint',         {type: 'bigint'}],
])

function globAsync(pattern: string, options: glob.IOptions = {}): Promise<string[]> {
  return new Promise((resolve, reject) => {
    glob(pattern, options, (err, matches) => {
      if (err) reject(err)
      resolve(matches)
    })
  })
}

function flatMap<T, R>(array: T[], iter: (item: T, index: number) => R[]): R[] {
  const acc: R[] = []
  for (let i = 0, len = array.length; i < len; i++) {
    const item = array[i]
    acc.push(...iter(item, i))
  }
  return acc
}

yargs
  .command('$0', 'Generate typed queries',
    (yargs) => {
      return yargs.options({
        sources: {
          description: 'Glob pattern of .edgeql files to generate types for',
          default: '**/*.edgeql',
          type: 'string'
        },
        dsn: {
          description: 'EdgeDB DSN string (edgedb://user:password@host:port/database)',
          type: 'string'
        }
      })
    },
    (argv) => {
      runTypegen(argv)
    }
  )
  .help()
  .argv

async function runTypegen({sources, dsn}: {sources: string, dsn?: string}) {
  const matches = await globAsync(sources, {ignore: '**/node_modules/**'})

  if (!matches.length) {
    console.log(chalk.yellow('No matching files'))
    return
  }

  let conn: AwaitConnection
  process.stdout.write('Connecting to database ... ')
  try {
    conn = await connect({dsn})
    process.stdout.write(chalk`{green Connected}\n`)
  } catch(e) {
    process.stdout.write(chalk`{red Failed}\n`)
    console.log(chalk.bgRed(e.toString()))
    return
  }

  console.log('Generating types...')
  for (const match of matches) {
    process.stdout.write(chalk`{blue ${match}} ... `)
    try {
      await processEdgeQLFile(match, conn)
      process.stdout.write(chalk`{green Done}\n`)
    } catch (e) {
      process.stdout.write(chalk`{red Failed}\n`)
      console.log(chalk.bgRed(e.toString()))
    }
  }
  
  conn.close()
}

async function processEdgeQLFile(path: string, conn: AwaitConnection) {
  const parsedPath = parsePath(path)
  const queryString = await readFileAsync(path, 'utf8')

  const [cardinality, inCodec, outCodec] = await conn._parse(queryString, false, false)

  const imports = new Set<string>(['TypedQuery'])
  const argType = generateArgType(inCodec, imports)
  let resultType = cardinality !== Cardinality.ZERO ? walkCodec(outCodec, imports) : null

  if (resultType && cardinality === Cardinality.MANY) {
    imports.add('Set')
    resultType = wrapType(resultType, 'Set')
  }

  const importsString = imports.size ?
    `import {${[...imports.values()].join(', ')}} from 'edgedb'\n\n` : ''
  const argTypeString = argType ? `export type ${parsedPath.name}Args = ${argType.join('\n')}\n\n` : ''
  const resultTypeString = resultType ? `export type ${parsedPath.name}Result = ${resultType.join('\n')}\n\n` : ''
  
  const queryExportString = `export const ${parsedPath.name}Query = {
  query: \`${queryString.trim().replace(/`/g, '\\`')}\`,
  expectOne: ${cardinality === Cardinality.ONE}
} as TypedQuery<${
    resultType ? parsedPath.name+'Result' : 'null'
  }${ argType ? ', '+parsedPath.name+'Args' : '' }>
export default ${parsedPath.name}Query`

  await writeFileAsync(
    formatPath({...parsedPath, base: undefined, ext: '.ts'}),
    importsString + argTypeString + resultTypeString + queryExportString
  )
}

function walkCodec(codec: ICodec, imports: Set<string>): string[] {
  if (codec instanceof ObjectCodec) {
    return generateObjectType(codec['names'], codec['codecs'], imports)
  }
  if (codec instanceof SetCodec) {
    imports.add('Set')
    const subType = walkCodec(codec['subCodec'], imports)
    return wrapType(subType, 'Set')
  }
  if (codec instanceof TupleCodec) {
    return generateTupleType(codec['subCodecs'], imports)
  }
  if (codec instanceof NamedTupleCodec) {
    const tupleType = generateTupleType(codec['subCodecs'], imports),
          objectType = generateObjectType(codec['names'], codec['subCodecs'], imports)
    return [
      ...tupleType.slice(0, -1),
      tupleType[tupleType.length-1] + ' & ' + objectType[0],
      ...objectType.slice(1)
    ]
  }
  if (codec instanceof EmptyTupleCodec) {
    return []
  }
  if (codec instanceof ArrayCodec) {
    const subType = walkCodec(codec['subCodec'], imports)
    return [
      ...subType.slice(0, -1),
      subType[subType.length-1] + '[]'
    ]
  }
  if (codec instanceof ScalarCodec) {
    if (codec instanceof EnumCodec) {
      return ['string']
    }
    const typeName = KNOWN_TYPES.get(codec.tid)
    if (!typeName) {
      throw new Error('Unknown scalar type')
    }
    if (!ScalarTypings.has(typeName)) {
      throw new Error(`No codec for scalar type ${typeName}`)
    }
    const {type, needsImport} = ScalarTypings.get(typeName)!

    if (needsImport) {
      imports.add(type)
    }
    return [type]
  }
  throw new Error('Unknown codec')
}

function generateArgType(codec: ICodec, imports: Set<string>): string[] | null {
  if (codec instanceof EmptyTupleCodec) return null
  if (codec instanceof TupleCodec) {
    return generateTupleType(codec['subCodecs'], imports)
  }
  if (codec instanceof NamedTupleCodec) {
    return generateObjectType(codec['names'], codec['subCodecs'], imports)
  }
  throw new Error('Unexpected arg codec')
}

function generateObjectType(names: string[], codecs: ICodec[], imports: Set<string>): string[] {
  return [
    '{',
    ...flatMap(names, (fieldName, i) => {
      const [firstLine, ...lines] = walkCodec(codecs[i], imports)
      return [
        `  ${fieldName}: ${firstLine || ''}`,
        ...lines.map(l => '  '+l)
      ]
    }),
    '}'
  ]
}

function generateTupleType(codecs: ICodec[], imports: Set<string>): string[] {
  return [
    '[',
    ...flatMap(codecs, (subCodec, i) => {
      return walkCodec(subCodec, imports)
        .map(l => '  '+l+(i!==(codecs.length-1)?',':''))
    }),
    ']'
  ]
}

function wrapType(type: string[], withType: string): string[] {
  return [
    withType+'<'+type[0] + (type.length === 1 ? '>' : ''),
    ...(type.length > 2 ? type.slice(1, -1) : []),
    ...(type.length > 1 ? [type[type.length-1]+'>'] : [])
  ]
}
