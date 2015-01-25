{-# LANGUAGE OverloadedStrings #-}
module Main where

import Data.Foldable (find)
import Data.List (unfoldr)
import Control.Applicative ((<$>), (<*>), (<*), (*>), (<|>), many)
import Data.Attoparsec.Text (parseOnly, many', string, try, (<?>), decimal, char, Parser, notChar, double)
import qualified Data.ByteString.Lazy.Char8 as BL
import Data.Int (Int64)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Lazy as TL
import qualified Data.Text.IO as T
import Data.Text.Lazy.Encoding (decodeUtf8)
import Data.Time.Calendar (Day, fromGregorian)
import Database.PostgreSQL.Simple (connect, Connection, defaultConnectInfo, ConnectInfo(..), execute, query, Only(..))
import Database.PostgreSQL.Simple.FromRow (FromRow(..), field)
import Database.PostgreSQL.Simple.ToField (ToField(..))
import Database.PostgreSQL.Simple.ToRow (ToRow(..))
import Network.HTTP (simpleHTTP, getRequest, getResponseBody)
import System.Environment (getArgs)
import System.IO (hClose)
import System.Process (runInteractiveProcess)

data Stock = Stock
             { stockDay :: Day
             , stockCode :: Text
             , stockName :: Text
             , stockMarket :: Text
             , stockCategory :: Text
             , stockOpeningPrice :: Maybe Double
             , stockHighPrice :: Maybe Double
             , stockLowPrice :: Maybe Double
             , stockClosingPrice :: Maybe Double
             , stockVolumeOfTrading :: Maybe Double
             , stockTradingValue :: Maybe Double
             }
           deriving Show

text :: Parser Text
text = many (notChar ',') >>= return . T.pack
code, name, market, category :: Parser Text
code = text
name = text
market = text
category = text
maybeDouble :: Parser (Maybe Double)
maybeDouble = fmap return double <|> fmap failure (char '-')
  where failure = const $ fail "read '-'"

comma :: Parser Char
comma = char ','
dash :: Parser Char
dash = char '-'

stock :: Day -> Parser Stock
stock d = Stock d <$> code
                  <*> (comma *> name)
                  <*> (comma *> market)
                  <*> (comma *> category)
                  <*> (comma *> maybeDouble)
                  <*> (comma *> maybeDouble)
                  <*> (comma *> maybeDouble)
                  <*> (comma *> maybeDouble)
                  <*> (comma *> maybeDouble)
                  <*> (comma *> maybeDouble)

int :: Parser Int
int = fmap fromInteger decimal
date :: Parser Day
date = fromGregorian <$> decimal <*> (dash *> int) <*> (dash *> int)

eol :: Parser Text
eol = try (string "\n\r") <|> try (string "\r\n") <|> string "\n" <|> string "\r" <?> "end of line"

header1 :: Parser Day
header1 = date <* (comma *> string "全銘柄日足") <* (comma *> string "http://k-db.com/")

header2 :: Parser ()
header2 = string "コード,銘柄名,市場,業種,始値,高値,安値,終値,出来高,売買代金" *> return ()

stocks :: Parser [Stock]
stocks = header1 <* eol <* header2 <* eol >>= \d -> many' (stock d <* eol)
  
fromLazy :: TL.Text -> Text
fromLazy = T.pack . TL.unpack

sjis2utf8 :: BL.ByteString -> IO BL.ByteString
sjis2utf8 s = do
  (inp, outp, _, _) <- runInteractiveProcess "iconv" ["-f","SHIFT_JIS","-t","UTF-8"] Nothing Nothing
  BL.hPutStr inp s
  hClose inp
  BL.hGetContents outp

mkURL :: String -> String
mkURL d = "http://k-db.com/?p=all&download=csv&date=" ++ d

main :: IO ()
main = do
  (ymd:_) <- getArgs
  rsp <- simpleHTTP $ getRequest $ mkURL ymd
  str <- getResponseBody rsp
  let sjis = BL.pack str
  utf8 <- sjis2utf8 sjis
  let utf8text = fromLazy $ decodeUtf8 utf8
  either putStr (mapM_ insert) $ parseOnly stocks utf8text

check :: IO ()
check = check' 0 10

check' :: Int -> Int -> IO ()
check' f t = do
  (ymd:_) <- getArgs
  rsp <- simpleHTTP $ getRequest $ mkURL ymd
  str <- getResponseBody rsp
  let sjis = BL.pack str
  utf8 <- sjis2utf8 sjis
  let utf8text = fromLazy $ decodeUtf8 utf8
  either putStr (mapM_ printStock . take t . drop f) $ parseOnly stocks utf8text

printStock :: Stock -> IO ()
printStock x = do
  T.putStr $ stockCode x `T.snoc` ','
  T.putStr $ stockName x `T.snoc` ','
  T.putStr $ stockMarket x `T.snoc` ','
  T.putStrLn $ stockCategory x

instance FromRow Stock where
  fromRow = Stock <$> field <*> field <*> field <*> field <*> field <*> field <*> field <*> field <*> field <*> field <*> field
instance ToRow Stock where
  toRow d = [ toField (stockDay d)
            , toField (stockCode d)
            , toField (stockName d)
            , toField (stockMarket d)
            , toField (stockCategory d)
            , toField (stockOpeningPrice d)
            , toField (stockHighPrice d)
            , toField (stockLowPrice d)
            , toField (stockClosingPrice d)
            , toField (stockVolumeOfTrading d)
            , toField (stockTradingValue d)
            ]


mkCon :: IO Connection
mkCon = connect defaultConnectInfo { connectUser = "cutsea110", connectPassword = "cutsea110", connectDatabase = "kabu" }

insert :: ToRow a => a -> IO Int64
insert x = do
  con <- mkCon
  execute con "insert into stock (day,code,name,market,category,openingprice,highprice,lowprice,closingprice,volumeoftrading,tradingvalue) values (?,?,?,?,?,?,?,?,?,?,?)" x

type Code = Text
type Name = Text

collect :: Either Code Name -> IO [Stock]
collect (Left cd) = do
  con <- mkCon
  query con "select * from stock where code = ?" (Only cd)
collect (Right nm) = do
  con <- mkCon
  query con "select * from stock where name = ?" (Only nm)

get :: Either Code Name -> IO (Maybe Stock)
get (Left cd) = do
  con <- mkCon
  fmap (find (const True)) $ query con "select * from stock where code = ? limit 1" (Only cd)
get (Right nm) = do
  con <- mkCon
  fmap (find (const True)) $ query con "select * from stock where name = ? limit 1" (Only nm)


ma :: Int -> [Int] -> [Int]
ma n = map (\xs -> sum xs `div` n) . filter (\xs -> length xs == n) . mset n

mset :: Int -> [a] -> [[a]]
mset n = unfoldr (\xs -> if null xs then Nothing else Just (take n xs, drop 1 xs))
