import chess
import chess.engine

def determine_next_move(board_state):
    board = chess.Board(board_state)
    # Use the built-in engine to get the best move
    with chess.engine.SimpleEngine.popen_uci("/usr/games/stockfish") as engine:
        result = engine.play(board, chess.engine.Limit(time=2.0))
    board.push(result.move)
    return board.fen()
