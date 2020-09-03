// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
// ------------------------------------------------------------

// Template generated code from Antlr4BuildTasks.Template v 3.0

namespace Microsoft.Azure.Cosmos.Query.Core.Parser
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using Antlr4.Runtime;
    using Antlr4.Runtime.Misc;

    internal sealed class ErrorListener<S> : IAntlrErrorListener<S>
    {
        public ParseException parseException;
        private readonly Parser parser;
        private readonly Lexer lexer;
        private readonly CommonTokenStream tokenStream;
        private bool firstTime;

        public ErrorListener(Parser parser, Lexer lexer, CommonTokenStream token_stream)
        {
            this.parser = parser;
            this.lexer = lexer;
            tokenStream = token_stream;
            firstTime = true;
        }

        public void SyntaxError(
            TextWriter output,
            IRecognizer recognizer,
            S offendingSymbol,
            int line,
            int col,
            string msg,
            RecognitionException recognitionException)
        {
            if (firstTime)
            {
                try
                {
                    firstTime = false;
                    LASets la_sets = new LASets();
                    IntervalSet set = la_sets.Compute(parser, tokenStream, line, col);
                    List<string> result = new List<string>();

                    foreach (int r in set.ToList())
                    {
                        string rule_name = parser.Vocabulary.GetSymbolicName(r);
                        result.Add(rule_name);
                    }

                    string message;
                    if (result.Any())
                    {
                        message = $"Parse error line:{line}, col:{col}, expecting: {string.Join(", ", result)}";

                    }
                    else
                    {
                        message = $"Parse error: message:{msg} offendingSymbol: {offendingSymbol}, line:{line}, col:{col}";
                    }

                    parseException = new ParseException(message, recognitionException);
                }
                catch (Exception ex)
                {
                    parseException = new ParseException($"Unknown parse exception", ex);
                }
            }
        }
    }
}