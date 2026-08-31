import { useState } from "react";
import "./index.css";


function App() {

  const [question, setQuestion] = useState("");
  const [messages, setMessages] = useState([]);
  const [loading, setLoading] = useState(false);


  const askQuestion = async (event) => {

    event.preventDefault();

    const currentQuestion = question.trim();

    if (!currentQuestion || loading) {
      return;
    }


    // Ajouter la question utilisateur

    setMessages((previousMessages) => [
      ...previousMessages,
      {
        type: "user",
        text: currentQuestion
      }
    ]);


    setQuestion("");

    setLoading(true);


    try {

      const response = await fetch(
        "http://localhost:8000/ask",
        {
          method: "POST",

          headers: {
            "Content-Type": "application/json"
          },

          body: JSON.stringify({
            question: currentQuestion,
            top_k: 5
          })
        }
      );


      if (!response.ok) {

        const error = await response.json();

        throw new Error(
          error.detail || "Erreur API"
        );
      }


      const data = await response.json();


      // Ajouter la réponse du RAG

      setMessages((previousMessages) => [
        ...previousMessages,
        {
          type: "assistant",
          text: data.answer,
          sources: data.sources || []
        }
      ]);

    }

    catch (error) {

      setMessages((previousMessages) => [
        ...previousMessages,
        {
          type: "error",
          text: error.message
        }
      ]);

    }

    finally {

      setLoading(false);

    }
  };


  return (

    <div className="app">


      {/* ================================================= */}
      {/* HEADER */}
      {/* ================================================= */}

      <header className="header">

        <h1>Bluesky RAG</h1>

        <p>
          Analyse des publications Bluesky
        </p>

      </header>


      {/* ================================================= */}
      {/* CHAT */}
      {/* ================================================= */}

      <main className="chat-container">


        {messages.length === 0 && (

          <div className="welcome">

            <h2>Posez une question</h2>

            <p>
              Le système recherchera les publications
              pertinentes dans Qdrant et générera une réponse
              avec Qwen.
            </p>

          </div>

        )}


        {messages.map((message, index) => (

          <div
            key={index}
            className={`message ${message.type}`}
          >

            <div className="message-label">

              {message.type === "user"
                ? "Vous"
                : message.type === "assistant"
                  ? "RAG"
                  : "Erreur"}

            </div>


            <div className="message-content">

              {message.text}

            </div>


            {/* SOURCES */}

            {message.sources &&
              message.sources.length > 0 && (

                <details className="sources">

                  <summary>
                    Sources utilisées
                    {" "}
                    ({message.sources.length})
                  </summary>


                  {message.sources.map(
                    (source, sourceIndex) => (

                      <div
                        className="source"
                        key={sourceIndex}
                      >

                        <div className="source-score">

                          Score :
                          {" "}
                          {Number(
                            source.score
                          ).toFixed(4)}

                        </div>


                        <div className="source-text">

                          {source.text}

                        </div>

                      </div>

                    )
                  )}

                </details>

              )}

          </div>

        ))}


        {loading && (

          <div className="message assistant">

            <div className="message-label">
              RAG
            </div>

            <div className="message-content">
              Recherche et génération en cours...
            </div>

          </div>

        )}

      </main>


      {/* ================================================= */}
      {/* QUESTION FORM */}
      {/* ================================================= */}

      <form
        className="question-form"
        onSubmit={askQuestion}
      >

        <input
          type="text"
          value={question}
          onChange={(event) =>
            setQuestion(event.target.value)
          }
          placeholder="Posez votre question..."
          disabled={loading}
        />


        <button
          type="submit"
          disabled={loading}
        >

          {loading
            ? "..."
            : "Envoyer"}

        </button>

      </form>


    </div>

  );

}


export default App;
