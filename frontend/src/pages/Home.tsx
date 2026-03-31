import { useNavigate } from "react-router-dom";
import { useState } from "react";
import { 
  Brain, 
  TrendingUp, 
  RefreshCw, 
  Database, 
  BarChart3, 
  Zap,
  CheckCircle2,
  ArrowRight,
  Target,
  Sparkles,
  Globe,
  Layers,
  Cpu,
  Clock,
  Server,
  Shield,
  Percent,
  Calendar,
  Settings,
  BarChart,
  Code
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import Header from "@/components/Header";

const Home = () => {
  const navigate = useNavigate();
  const [selectedFeature, setSelectedFeature] = useState(0);

  return (
    <div className="min-h-screen bg-background">
      <Header />
      
      {/* Hero Section */}
      <section className="relative overflow-hidden py-16 sm:py-20 md:py-24 lg:py-28 px-4 md:px-6">
        {/* Background Image */}
        <div 
          className="absolute inset-0"
          style={{
            backgroundImage: 'url(/hero_bg.png)',
            backgroundSize: 'cover',
            backgroundPosition: 'center',
            backgroundRepeat: 'no-repeat',
          }}
        ></div>
        
        {/* Dark overlay for better text readability */}
        <div className="absolute inset-0 bg-black/55"></div>
        
        {/* Subtle gradient overlay with project colors */}
        <div className="absolute inset-0 bg-gradient-to-br from-prediction-blue/10 via-transparent to-prediction-orange/10"></div>
        
        {/* Content Container */}
        <div className="relative z-10 max-w-4xl mx-auto text-center pt-12 pb-16">
          {/* Badge */}
          <div className="inline-flex items-center gap-2 px-4 py-2 rounded-full bg-white/85 backdrop-blur-md border border-white/50 text-prediction-blue mb-6 shadow-md">
            <Sparkles className="w-3.5 h-3.5" />
            <span className="text-xs font-semibold tracking-wide uppercase" style={{ fontFamily: "'Lato', sans-serif" }}>AI-Driven Sports Analytics</span>
          </div>
          
          {/* Main Heading */}
          <h1 className="text-4xl sm:text-5xl md:text-6xl lg:text-7xl font-bold text-white mb-5 leading-tight tracking-tight" style={{ fontFamily: "'Bebas Neue', sans-serif", textShadow: '0 2px 10px rgba(0, 0, 0, 0.7), 0 1px 3px rgba(0, 0, 0, 0.5)' }}>
            AI-Driven Sports Betting &<br />
            <span className="bg-gradient-to-r from-prediction-blue to-prediction-orange bg-clip-text text-transparent" style={{ textShadow: '0 2px 6px rgba(0, 0, 0, 0.6)' }}>NFL Match Predictions</span>
          </h1>
          
          {/* Description */}
          <p className="text-base sm:text-lg md:text-xl text-white/95 max-w-2xl mx-auto mb-8 leading-relaxed" style={{ fontFamily: "'Lato', sans-serif", textShadow: '0 1px 4px rgba(0, 0, 0, 0.6)' }}>
            A powerful analytics system built for bettors, analysts, and enthusiasts — 
            delivering accurate, data-driven NFL game predictions using advanced machine learning.
          </p>
          
          {/* CTA Button */}
          <div className="flex justify-center">
            <Button 
              size="lg" 
              className="group relative text-base sm:text-lg font-semibold px-10 py-7 bg-prediction-blue hover:bg-prediction-blue/95 text-white border-2 border-prediction-blue/30 rounded-xl shadow-2xl hover:shadow-prediction-blue/50 transition-all duration-300 hover:scale-105 hover:border-prediction-blue/50"
              style={{ 
                fontFamily: "'Lato', sans-serif",
                boxShadow: '0 10px 25px -5px rgba(74, 158, 255, 0.4), 0 4px 6px -2px rgba(0, 0, 0, 0.3)'
              }}
              onClick={() => navigate("/predictions")}
            >
              <span className="relative z-10 flex items-center">
                View Predictions
                <ArrowRight className="w-5 h-5 ml-2 transition-transform duration-300 group-hover:translate-x-1" />
              </span>
              {/* Shine effect on hover */}
              <span className="absolute inset-0 rounded-xl bg-gradient-to-r from-transparent via-white/20 to-transparent opacity-0 group-hover:opacity-100 group-hover:animate-shimmer transition-opacity duration-300"></span>
            </Button>
          </div>
        </div>
      </section>

      {/* Section 1: What This System Does */}
      <section className="py-20 px-4 bg-card/50">
        <div className="max-w-7xl mx-auto">
          <div className="text-center mb-16">
            <h2 className="text-3xl md:text-4xl lg:text-5xl font-bold text-foreground mb-4 leading-tight">
              Predict Every NFL Game Winner<br className="hidden sm:block" /> With Data-Driven Confidence
            </h2>
            <p className="text-lg text-muted-foreground max-w-3xl mx-auto">
              Our platform analyzes team features, statistical attributes, and advanced metrics 
              to forecast the outcome of upcoming NFL games.
            </p>
          </div>
          
          <div className="grid md:grid-cols-2 lg:grid-cols-4 gap-6">
            {[
              {
                icon: Target,
                title: "Accurate Win Predictions",
                description: "Machine learning-powered forecasts for every NFL matchup"
              },
              {
                icon: BarChart3,
                title: "Confidence Percentages",
                description: "Clear probability scores for each team's winning chances"
              },
              {
                icon: TrendingUp,
                title: "Team Comparison Insights",
                description: "Deep statistical analysis comparing team performance"
              },
              {
                icon: Database,
                title: "Up-to-Date Match Data",
                description: "Real-time data via custom APIs for the latest information"
              }
            ].map((feature, index) => (
              <Card key={index} className="border-border hover:shadow-lg transition-shadow">
                <CardContent className="p-6">
                  <div className={`w-12 h-12 rounded-lg ${index % 2 === 0 ? 'bg-prediction-blue/10' : 'bg-prediction-orange/10'} flex items-center justify-center mb-4`}>
                    <feature.icon className={`w-6 h-6 ${index % 2 === 0 ? 'text-prediction-blue' : 'text-prediction-orange'}`} />
                  </div>
                  <h3 className="text-xl font-semibold text-foreground mb-2">
                    {feature.title}
                  </h3>
                  <p className="text-muted-foreground">
                    {feature.description}
                  </p>
                </CardContent>
              </Card>
            ))}
          </div>
          
          <div className="mt-12 text-center">
            <p className="text-lg text-foreground font-medium">
              The result: <span className="text-prediction-blue font-bold">smarter betting decisions</span> backed by machine learning.
            </p>
          </div>
        </div>
      </section>

      {/* Section 2: How the System Works */}
      <section className="py-20 px-4 bg-background">
        <div className="max-w-7xl mx-auto">
          <div className="text-center mb-16">
            <h2 className="text-4xl md:text-5xl font-bold text-foreground mb-4">
              How Our Prediction Model Works
            </h2>
          </div>

          <div className="grid lg:grid-cols-2 gap-8 items-stretch">
            {/* Left Side - Feature List */}
            <div className="space-y-4 flex flex-col">
              {[
                {
                  id: 0,
                  icon: Database,
                  title: "Feature-Rich Training Data",
                  color: "prediction-blue"
                },
                {
                  id: 1,
                  icon: Brain,
                  title: "Machine Learning Prediction Engine",
                  color: "prediction-orange"
                },
                {
                  id: 2,
                  icon: RefreshCw,
                  title: "Automated Weekly Retraining",
                  color: "prediction-blue"
                },
                {
                  id: 3,
                  icon: Server,
                  title: "Real-Time Match Data APIs",
                  color: "prediction-orange"
                }
              ].map((feature) => {
                const isSelected = selectedFeature === feature.id;
                const isBlue = feature.color === "prediction-blue";
                
                return (
                  <Card
                    key={feature.id}
                    className={`border-2 cursor-pointer transition-all duration-300 ${
                      isSelected
                        ? isBlue
                          ? "border-prediction-blue bg-prediction-blue/5 shadow-lg"
                          : "border-prediction-orange bg-prediction-orange/5 shadow-lg"
                        : "border-border hover:border-border/60 hover:shadow-md"
                    }`}
                    onClick={() => setSelectedFeature(feature.id)}
                  >
                    <CardContent className="p-6">
                      <div className="flex items-center gap-4">
                        <div className={`w-12 h-12 rounded-lg flex items-center justify-center flex-shrink-0 ${
                          isBlue ? "bg-prediction-blue/10" : "bg-prediction-orange/10"
                        }`}>
                          <feature.icon className={`w-6 h-6 ${
                            isBlue ? "text-prediction-blue" : "text-prediction-orange"
                          }`} />
                        </div>
                        <h3 className={`text-lg font-bold ${
                          isSelected
                            ? isBlue
                              ? "text-prediction-blue"
                              : "text-prediction-orange"
                            : "text-foreground"
                        }`}>
                          {feature.title}
                        </h3>
                      </div>
                    </CardContent>
                  </Card>
                );
              })}
            </div>

            {/* Right Side - Content Display */}
            <div className="lg:sticky lg:top-8 flex">
              <Card className="border-border flex-1 flex flex-col">
                <CardContent className="p-8 flex-1 flex flex-col">
                  {selectedFeature === 0 && (
                    <div className="flex items-start gap-4 flex-1">
                      <div className="w-14 h-14 rounded-lg bg-prediction-blue/10 flex items-center justify-center flex-shrink-0">
                        <Database className="w-7 h-7 text-prediction-blue" />
                      </div>
                      <div>
                        <h3 className="text-2xl font-bold text-foreground mb-3">
                          Feature-Rich Training Data
                        </h3>
                        <p className="text-muted-foreground mb-4">
                          Our model is trained on a large set of features, including:
                        </p>
                        <ul className="space-y-2">
                          {[
                            "Team performance metrics",
                            "Standard statistical features",
                            "Advanced statistical features (Glicko rating, power ranking, etc.)",
                            "Historic match results",
                            "Strength-of-schedule factors"
                          ].map((item, index) => (
                            <li key={index} className="flex items-start gap-2">
                              <CheckCircle2 className="w-5 h-5 text-prediction-blue mt-0.5 flex-shrink-0" />
                              <span className="text-muted-foreground">{item}</span>
                            </li>
                          ))}
                        </ul>
                        <p className="text-foreground mt-4 font-medium">
                          These inputs allow the model to capture real performance patterns and predict outcomes more reliably.
                        </p>
                      </div>
                    </div>
                  )}

                  {selectedFeature === 1 && (
                    <div className="flex items-start gap-4 flex-1">
                      <div className="w-14 h-14 rounded-lg bg-prediction-orange/10 flex items-center justify-center flex-shrink-0">
                        <Brain className="w-7 h-7 text-prediction-orange" />
                      </div>
                      <div>
                        <h3 className="text-2xl font-bold text-foreground mb-3">
                          Machine Learning Prediction Engine
                        </h3>
                        <p className="text-muted-foreground mb-4">
                          We use supervised ML techniques to classify which team is more likely to win.
                        </p>
                        <div className="bg-gradient-to-r from-prediction-blue/10 to-prediction-orange/10 rounded-lg p-4 mt-4">
                          <p className="text-foreground font-semibold">
                            The model currently achieves <span className="bg-gradient-to-r from-prediction-blue to-prediction-orange bg-clip-text text-transparent text-2xl">~70%</span> prediction accuracy, 
                            based on historical evaluation.
                          </p>
                        </div>
                      </div>
                    </div>
                  )}

                  {selectedFeature === 2 && (
                    <div className="flex items-start gap-4 flex-1">
                      <div className="w-14 h-14 rounded-lg bg-prediction-blue/10 flex items-center justify-center flex-shrink-0">
                        <RefreshCw className="w-7 h-7 text-prediction-blue" />
                      </div>
                      <div>
                        <h3 className="text-2xl font-bold text-foreground mb-3">
                          Automated Weekly Retraining
                        </h3>
                        <p className="text-muted-foreground mb-4">
                          A scheduled training script automatically retrains the model:
                        </p>
                        <ul className="space-y-2">
                          {[
                            "Every Wednesday at 12 AM",
                            "On new, updated match and team data",
                            "Ensuring the model stays accurate and adapts to recent performance trends"
                          ].map((item, index) => (
                            <li key={index} className="flex items-start gap-2">
                              <RefreshCw className="w-5 h-5 text-prediction-blue mt-0.5 flex-shrink-0" />
                              <span className="text-muted-foreground">{item}</span>
                            </li>
                          ))}
                        </ul>
                      </div>
                    </div>
                  )}

                  {selectedFeature === 3 && (
                    <div className="flex items-start gap-4 flex-1">
                      <div className="w-14 h-14 rounded-lg bg-prediction-orange/10 flex items-center justify-center flex-shrink-0">
                        <Server className="w-7 h-7 text-prediction-orange" />
                      </div>
                      <div>
                        <h3 className="text-2xl font-bold text-foreground mb-3">
                          Real-Time Match Data APIs
                        </h3>
                        <p className="text-muted-foreground mb-4">
                          Custom APIs provide:
                        </p>
                        <ul className="space-y-2">
                          {[
                            "Upcoming match schedules",
                            "Team statistics",
                            "Prediction outputs",
                            "Probability scores"
                          ].map((item, index) => (
                            <li key={index} className="flex items-start gap-2">
                              <Globe className="w-5 h-5 text-prediction-orange mt-0.5 flex-shrink-0" />
                              <span className="text-muted-foreground">{item}</span>
                            </li>
                          ))}
                        </ul>
                        <p className="text-foreground mt-4 font-medium">
                          These APIs power both internal dashboards and external client applications.
                        </p>
                      </div>
                    </div>
                  )}
                </CardContent>
              </Card>
            </div>
          </div>
        </div>
      </section>

      {/* Section 3: Why It Matters */}
      <section className="py-20 px-4 bg-card/50">
        <div className="max-w-7xl mx-auto">
          <div className="text-center mb-16">
            <h2 className="text-4xl md:text-5xl font-bold text-foreground mb-4">
              Why Bettors & Analysts Use This System
            </h2>
          </div>

          <div className="grid md:grid-cols-2 lg:grid-cols-3 gap-8 lg:gap-10 items-start">
            {[
              {
                title: "Consistent, Unbiased Predictions",
                description: "Data-driven forecasts free from human bias, ensuring objective analysis for every NFL matchup.",
                icon: Shield,
                color: "prediction-blue"
              },
              {
                title: "Clear Win Probability Percentages",
                description: "Transparent probability scores that help you understand each team's winning chances with precision.",
                icon: Percent,
                color: "prediction-orange"
              },
              {
                title: "Up-to-Date Weekly Accuracy",
                description: "Real-time accuracy tracking that updates weekly, showing how well our predictions perform.",
                icon: Calendar,
                color: "prediction-blue"
              },
              {
                title: "Fully Automated Model Training",
                description: "Self-updating machine learning models that retrain automatically every week without manual intervention.",
                icon: Settings,
                color: "prediction-orange"
              },
              {
                title: "Deep Statistical Insights",
                description: "Advanced metrics including Glicko ratings, power rankings, and comprehensive team performance analytics.",
                icon: BarChart,
                color: "prediction-blue"
              },
              {
                title: "Custom APIs for Integration",
                description: "RESTful APIs that allow seamless integration into your own tools, dashboards, or applications.",
                icon: Code,
                color: "prediction-orange"
              }
            ].map((benefit, index) => {
              const isBlue = benefit.color === "prediction-blue";
              return (
                <div key={index} className="flex flex-col">
                  <div className="flex items-center gap-3 mb-3">
                    <div className={`w-11 h-11 rounded-lg flex items-center justify-center flex-shrink-0 ${
                      isBlue ? "bg-prediction-blue/10" : "bg-prediction-orange/10"
                    }`}>
                      <benefit.icon className={`w-5 h-5 ${
                        isBlue ? "text-prediction-blue" : "text-prediction-orange"
                      }`} />
                    </div>
                    <div className="flex-1 min-w-0">
                      <h3 className={`text-lg font-bold leading-tight ${
                        isBlue ? "text-prediction-blue" : "text-prediction-orange"
                      }`}>
                        {benefit.title}
                      </h3>
                    </div>
                  </div>
                  <p className="text-muted-foreground text-sm leading-relaxed">
                    {benefit.description}
                  </p>
                </div>
              );
            })}
          </div>

          <div className="mt-12 text-center">
            <p className="text-xl text-foreground">
              Whether you're a sports bettor or a data analyst, the platform gives you{" "}
              <span className="bg-gradient-to-r from-prediction-blue to-prediction-orange bg-clip-text text-transparent font-bold">data-backed decision support</span>.
            </p>
          </div>
        </div>
      </section>

      {/* Section 4: System Highlights */}
      <section className="py-20 px-4 bg-background">
        <div className="max-w-7xl mx-auto">
          <div className="text-center mb-16">
            <h2 className="text-4xl md:text-5xl font-bold text-foreground mb-4">
              System Highlights
            </h2>
          </div>

          <div className="grid md:grid-cols-2 lg:grid-cols-3 gap-6">
            {[
              {
                icon: Brain,
                title: "Machine Learning Model",
                description: "Trained on diverse statistical and advanced features"
              },
              {
                icon: TrendingUp,
                title: "70% Accuracy",
                description: "Achieved in testing"
              },
              {
                icon: RefreshCw,
                title: "Weekly Retraining",
                description: "For continuous performance improvement"
              },
              {
                icon: Server,
                title: "Live APIs",
                description: "For match data and predictions"
              },
              {
                icon: Zap,
                title: "Automated Pipelines",
                description: "For data ingestion & feature engineering"
              },
              {
                icon: BarChart3,
                title: "Sports Analytics Focus",
                description: "For bettors and researchers"
              }
            ].map((highlight, index) => (
              <Card key={index} className={`border-border hover:shadow-lg transition-all ${index % 2 === 0 ? 'hover:border-prediction-blue/50' : 'hover:border-prediction-orange/50'}`}>
                <CardContent className="p-6">
                  <div className={`w-14 h-14 rounded-lg ${index % 2 === 0 ? 'bg-prediction-blue/10' : 'bg-prediction-orange/10'} flex items-center justify-center mb-4`}>
                    <highlight.icon className={`w-7 h-7 ${index % 2 === 0 ? 'text-prediction-blue' : 'text-prediction-orange'}`} />
                  </div>
                  <h3 className="text-xl font-bold text-foreground mb-2">
                    {highlight.title}
                  </h3>
                  <p className="text-muted-foreground">
                    {highlight.description}
                  </p>
                </CardContent>
              </Card>
            ))}
          </div>
        </div>
      </section>

      {/* CTA Section */}
      <section className="py-20 px-4 bg-gradient-to-br from-prediction-blue/10 via-background to-prediction-orange/10">
        <div className="max-w-4xl mx-auto text-center">
          <h2 className="text-4xl md:text-5xl font-bold text-foreground mb-6">
            Ready to Get Started?
          </h2>
          <p className="text-xl text-muted-foreground mb-8">
            Explore our predictions and see how data-driven insights can improve your decision-making.
          </p>
          <Button 
            size="lg" 
            className="text-lg px-8 py-6 bg-prediction-blue hover:bg-prediction-blue/90 text-white border-0 shadow-lg hover:shadow-xl transition-all duration-300"
            onClick={() => navigate("/predictions")}
          >
            View NFL Predictions
            <ArrowRight className="w-5 h-5 ml-2" />
          </Button>
        </div>
      </section>

      {/* Footer */}
      <footer className="bg-card border-t border-border py-16 px-4">
        <div className="max-w-7xl mx-auto">
          <div className="text-center space-y-6">
            <div className="flex items-center justify-center gap-3">
              <img
                src="/logo.png"
                alt="NFL Logo"
                className="w-12 h-12 rounded-full object-cover border border-border bg-card"
              />
              <h3 className="text-2xl font-bold text-foreground">
                NFL Match Predictions
              </h3>
            </div>
            <p className="text-lg text-muted-foreground max-w-2xl mx-auto">
              Powered by advanced machine learning and statistical analysis to deliver 
              accurate, data-driven NFL game predictions.
            </p>
            <div className="flex items-center justify-center gap-2 text-sm text-muted-foreground">
              <span>© 2025</span>
              <span>•</span>
              <span>Sports Analytics Prediction System</span>
              <span>•</span>
              <span>All rights reserved</span>
            </div>
          </div>
        </div>
      </footer>
    </div>
  );
};

export default Home;

