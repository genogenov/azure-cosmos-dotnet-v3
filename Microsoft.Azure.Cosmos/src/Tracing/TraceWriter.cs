﻿// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
// ------------------------------------------------------------

namespace Microsoft.Azure.Cosmos.Tracing
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;

    internal static class TraceWriter
    {
        private const string space = "  ";

        private static readonly Dictionary<AsciiType, AsciiTreeCharacters> asciiTreeCharactersMap = new Dictionary<AsciiType, AsciiTreeCharacters>()
        {
            {
                AsciiType.Default,
                new AsciiTreeCharacters(
                    blank: ' ',
                    child: '├',
                    dash: '─',
                    last: '└',
                    parent: '│',
                    root: '.')
            },
            {
                AsciiType.DoubleLine,
                new AsciiTreeCharacters(
                    blank: ' ',
                    child: '╠',
                    dash: '═',
                    last: '╚',
                    parent: '║',
                    root: '╗')
            },
            {
                AsciiType.Classic,
                new AsciiTreeCharacters(
                    blank: ' ',
                    child: '|',
                    dash: '-',
                    last: '+',
                    parent: '|',
                    root: '+')
            },
            {
                AsciiType.ClassicRounded,
                new AsciiTreeCharacters(
                    blank: ' ',
                    child: '|',
                    dash: '-',
                    last: '`',
                    parent: '|',
                    root: '+')
            },
            {
                AsciiType.ExclamationMarks,
                new AsciiTreeCharacters(
                    blank: ' ',
                    child: '#',
                    dash: '=',
                    last: '*',
                    parent: '!',
                    root: '#')
            },
        };
        private static readonly Dictionary<AsciiType, AsciiTreeIndents> asciiTreeIndentsMap = new Dictionary<AsciiType, AsciiTreeIndents>()
        {
            { AsciiType.Default, AsciiTreeIndents.Create(asciiTreeCharactersMap[AsciiType.Default]) },
            { AsciiType.DoubleLine, AsciiTreeIndents.Create(asciiTreeCharactersMap[AsciiType.DoubleLine]) },
            { AsciiType.Classic, AsciiTreeIndents.Create(asciiTreeCharactersMap[AsciiType.Classic]) },
            { AsciiType.ClassicRounded, AsciiTreeIndents.Create(asciiTreeCharactersMap[AsciiType.ClassicRounded]) },
            { AsciiType.ExclamationMarks, AsciiTreeIndents.Create(asciiTreeCharactersMap[AsciiType.ExclamationMarks]) },
        };

        private static readonly string[] newLines = new string[] { Environment.NewLine };
        private static readonly char[] newLineCharacters = Environment.NewLine.ToCharArray();

        public static void WriteTrace(
            TextWriter writer,
            ITrace trace,
            TraceLevel level = TraceLevel.Verbose,
            AsciiType asciiType = AsciiType.Default)
        {
            if (writer == null)
            {
                throw new ArgumentNullException(nameof(writer));
            }

            if (trace == null)
            {
                throw new ArgumentNullException(nameof(trace));
            }

            if ((int)trace.Level < (int)level)
            {
                return;
            }

            AsciiTreeCharacters asciiTreeCharacter = asciiTreeCharactersMap[asciiType];
            AsciiTreeIndents asciiTreeIndents = asciiTreeIndentsMap[asciiType];

            writer.WriteLine(asciiTreeCharacter.Root);
            WriteTraceRecursive(writer, trace, level, asciiTreeIndents, isLastChild: true);
        }

        private static void WriteTraceRecursive(
            TextWriter writer,
            ITrace trace,
            TraceLevel level,
            AsciiTreeIndents asciiTreeIndents,
            bool isLastChild)
        {
            ITrace parent = trace.Parent;
            Stack<string> indentStack = new Stack<string>();
            while (parent != null)
            {
                bool parentIsLastChild = (parent.Parent == null) || parent.Equals(parent.Parent.Children.Last());
                if (parentIsLastChild)
                {
                    indentStack.Push(asciiTreeIndents.Blank);
                }
                else
                {
                    indentStack.Push(asciiTreeIndents.Parent);
                }

                parent = parent.Parent;
            }

            WriteIndents(writer, indentStack, asciiTreeIndents, isLastChild);

            writer.Write(trace.Name);
            writer.Write('(');
            writer.Write(trace.Id);
            writer.Write(')');
            writer.Write(space);

            writer.Write(trace.Component);
            writer.Write('-');
            writer.Write("Component");
            writer.Write(space);

            writer.Write(trace.StackFrame.GetFileName().Split('\\').Last());
            writer.Write(':');
            writer.Write(trace.StackFrame.GetFileLineNumber());
            writer.Write(space);

            writer.Write(trace.StartTime.ToString("hh:mm:ss:fff"));
            writer.Write(space);

            writer.Write(trace.Duration.TotalMilliseconds.ToString("0.00"));
            writer.Write(" milliseconds");
            writer.Write(space);

            writer.WriteLine();

            if (trace.Info != null)
            {
                bool isLeaf = trace.Children.Count == 0;

                WriteInfoIndents(writer, indentStack, asciiTreeIndents, isLastChild: isLastChild, isLeaf: isLeaf);
                writer.WriteLine('(');

                string[] infoLines = trace.Info
                    .Serialize()
                    .TrimEnd(newLineCharacters)
                    .Split(newLines, StringSplitOptions.None);
                foreach (string infoLine in infoLines)
                {
                    WriteInfoIndents(writer, indentStack, asciiTreeIndents, isLastChild: isLastChild, isLeaf: isLeaf);
                    writer.Write(asciiTreeIndents.Blank);
                    writer.WriteLine(infoLine);
                }

                WriteInfoIndents(writer, indentStack, asciiTreeIndents, isLastChild: isLastChild, isLeaf: isLeaf);
                writer.WriteLine(')');
            }

            for (int i = 0; i < trace.Children.Count - 1; i++)
            {
                ITrace child = trace.Children[i];
                WriteTraceRecursive(writer, child, level, asciiTreeIndents, isLastChild: false);
            }

            if (trace.Children.Count != 0)
            {
                ITrace child = trace.Children[trace.Children.Count - 1];
                WriteTraceRecursive(writer, child, level, asciiTreeIndents, isLastChild: true);
            }
        }

        private static void WriteIndents(
            TextWriter writer,
            Stack<string> indentStack,
            AsciiTreeIndents asciiTreeIndents,
            bool isLastChild)
        {
            foreach (string indent in indentStack)
            {
                writer.Write(indent);
            }

            if (isLastChild)
            {
                writer.Write(asciiTreeIndents.Last);
            }
            else
            {
                writer.Write(asciiTreeIndents.Child);
            }
        }

        private static void WriteInfoIndents(
            TextWriter writer,
            Stack<string> indentStack,
            AsciiTreeIndents asciiTreeIndents,
            bool isLastChild,
            bool isLeaf)
        {
            foreach (string indent in indentStack)
            {
                writer.Write(indent);
            }

            if (isLastChild)
            {
                writer.Write(asciiTreeIndents.Blank);
            }
            else
            {
                writer.Write(asciiTreeIndents.Parent);
            }

            if (isLeaf)
            {
                writer.Write(asciiTreeIndents.Blank);
            }
            else
            {
                writer.Write(asciiTreeIndents.Parent);
            }
        }

        /// <summary>
        /// Character set to generate an Ascii Tree (https://marketplace.visualstudio.com/items?itemName=aprilandjan.ascii-tree-generator)
        /// </summary>
        private readonly struct AsciiTreeCharacters
        {
            public AsciiTreeCharacters(char blank, char child, char dash, char last, char parent, char root)
            {
                this.Blank = blank;
                this.Child = child;
                this.Dash = dash;
                this.Last = last;
                this.Parent = parent;
                this.Root = root;
            }

            /// <summary>
            /// For blanks / spaces
            /// </summary>
            public char Blank { get; }

            /// <summary>
            /// For intermediate child elements
            /// </summary>
            public char Child { get; }

            /// <summary>
            /// For horizontal dashes
            /// </summary>
            public char Dash { get; }

            /// <summary>
            /// For the last element of a path
            /// </summary>
            public char Last { get; }

            /// <summary>
            /// For vertical parent elements
            /// </summary>
            public char Parent { get; }

            /// <summary>
            /// For the root element (on top)
            /// </summary>
            public char Root { get; }
        }

        private readonly struct AsciiTreeIndents
        {
            private AsciiTreeIndents(string child, string parent, string last, string blank)
            {
                this.Child = child;
                this.Parent = parent;
                this.Last = last;
                this.Blank = blank;
            }

            public string Child { get; }

            public string Parent { get; }

            public string Last { get; }

            public string Blank { get; }

            public static AsciiTreeIndents Create(AsciiTreeCharacters asciiTreeCharacters) => new AsciiTreeIndents(
                child: new string(
                    new char[]
                    {
                        asciiTreeCharacters.Child,
                        asciiTreeCharacters.Dash,
                        asciiTreeCharacters.Dash,
                        asciiTreeCharacters.Blank
                    }),
                parent: new string(
                    new char[]
                    {
                        asciiTreeCharacters.Parent,
                        asciiTreeCharacters.Blank,
                        asciiTreeCharacters.Blank,
                        asciiTreeCharacters.Blank
                    }),
                last: new string(
                    new char[]
                    {
                        asciiTreeCharacters.Last,
                        asciiTreeCharacters.Dash,
                        asciiTreeCharacters.Dash,
                        asciiTreeCharacters.Blank
                    }),
                blank: new string(
                    new char[]
                    {
                        asciiTreeCharacters.Blank,
                        asciiTreeCharacters.Blank,
                        asciiTreeCharacters.Blank,
                        asciiTreeCharacters.Blank
                    }));
        }

        public enum AsciiType
        {
            Default,
            DoubleLine,
            Classic,
            ClassicRounded,
            ExclamationMarks,
        }
    }
}
